#!/usr/bin/env python3
# csv2db.py — Phase B (redo): schema + simple mappings. 251109.172600
# Purpose: load config, load simple mappings (source_column,target_column),
# bootstrap SQLite schema, auto-widen columns. No ingest yet.

from __future__ import annotations
import argparse, csv, json, re, sqlite3, sys
from pathlib import Path
from datetime import datetime, timezone

# -----------------------------
# [SECTION: UTIL]
# -----------------------------
APP_TS = datetime.utcnow().strftime("%y%m%d.%H%M%S")

def utc_now_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f +0000")

def info(msg: str) -> None:
    sys.stdout.write(f"[{APP_TS}] {msg}\n"); sys.stdout.flush()

def die(msg: str, code: int = 1) -> None:
    sys.stderr.write(f"[{APP_TS}] ERROR: {msg}\n"); sys.stderr.flush(); sys.exit(code)

# -----------------------------
# [SECTION: ARGPARSE]
# -----------------------------
def build_args():
    p = argparse.ArgumentParser(description="CSV → SQLite wide-table consolidator (Phase B, simple mappings)")
    p.add_argument("--config", default="config/app.config.json", help="Path to app.config.json")
    p.add_argument("--mappings", default="config/mappings.config.csv", help="Path to mappings CSV (source_column,target_column)")
    p.add_argument("--initdb", action="store_true", help="Create DB and apply schema only")
    p.add_argument("--show-mappings", action="store_true", help="Print mapping summary and exit")
    return p.parse_args()

# -----------------------------
# [SECTION: CONFIG]
# -----------------------------
def load_config(path: Path) -> dict:
    if not path.exists():
        die(f"Missing config: {path}")
    with path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("utc", True)
    cfg.setdefault("pattern", "*.csv")   # still used by later phases
    cfg.setdefault("source_dir", "source")
    cfg.setdefault("target_db", "target/warehouse.sqlite")
    cfg.setdefault("wal", True)
    cfg.setdefault("pragma", {})
    return cfg

# -----------------------------
# [SECTION: MAPPINGS]
# -----------------------------
def load_mappings(path: Path) -> list[tuple[str,str]]:
    """
    Returns list of tuples: (source_column, target_column)
    Lines starting with '#' are ignored. Applies to ALL input files.
    """
    if not path.exists():
        die(f"Missing mappings: {path}")
    pairs: list[tuple[str,str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.reader(f)
        for raw in rdr:
            if not raw: 
                continue
            if str(raw[0]).strip().startswith("#"):
                continue
            # normalize to two fields
            row = [c.strip() for c in raw[:2]]
            if len(row) < 2 or not row[0] or not row[1]:
                continue
            src_col, tgt_col = row[0], row[1]
            pairs.append((src_col, tgt_col))
    if not pairs:
        die("No valid source→target mappings found.")
    return pairs

def mapping_targets(mappings: list[tuple[str,str]]) -> list[str]:
    # Ordered unique target columns
    seen, out = set(), []
    for _, tgt in mappings:
        if tgt not in seen:
            seen.add(tgt); out.append(tgt)
    return out

# -----------------------------
# [SECTION: DB_BOOTSTRAP]
# -----------------------------
BASE_COLUMNS = [
    ("id", "INTEGER PRIMARY KEY"),
    ("src_file", "TEXT"),
    ("src_line", "INTEGER"),
    ("raw_hash", "TEXT"),
    ("map_hash", "TEXT"),
    ("loaded_at_utc", "TEXT"),
    ("ingest_ver", "TEXT")
]
UNIQUE_CONSTRAINTS = [
    ("raw_hash",)
]

def connect_db(db_path: Path, wal: bool, pragma: dict) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON;")
    if wal:
        conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(f"PRAGMA synchronous={pragma.get('synchronous','NORMAL')};")
    if pragma.get("temp_store", "MEMORY") == "MEMORY":
        conn.execute("PRAGMA temp_store=MEMORY;")
    mmap = int(pragma.get("mmap_size", 0))
    if mmap > 0:
        conn.execute(f"PRAGMA mmap_size={mmap};")
    cache_kb = int(pragma.get("cache_size_kb", 0))
    if cache_kb > 0:
        conn.execute(f"PRAGMA cache_size=-{cache_kb};")
    return conn

def ensure_raw_events(conn: sqlite3.Connection) -> None:
    cols_sql = ", ".join([f"{name} {ctype}" for name, ctype in BASE_COLUMNS])
    conn.execute(f"CREATE TABLE IF NOT EXISTS raw_events ({cols_sql});")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_events_raw_hash ON raw_events(raw_hash);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_raw_events_src_file ON raw_events(src_file);")
    conn.commit()

# -----------------------------
# [SECTION: WIDEN_TABLE]
# -----------------------------
def current_columns(conn: sqlite3.Connection, table: str = "raw_events") -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return {row[1] for row in cur.fetchall()}

_COLNAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def widen_table(conn: sqlite3.Connection, targets: list[str]) -> list[str]:
    existing = current_columns(conn)
    added = []
    for col in targets:
        if col in existing:
            continue
        if not _COLNAME_RE.match(col):
            die(f"Invalid target_column name in mappings: '{col}'")
        conn.execute(f"ALTER TABLE raw_events ADD COLUMN {col} TEXT;")
        added.append(col)
    if added:
        conn.commit()
    return added

# -----------------------------
# [SECTION: MAIN]
# -----------------------------
def main():
    args = build_args()
    cfg = load_config(Path(args.config))
    pairs = load_mappings(Path(args.mappings))
    targets = mapping_targets(pairs)

    info(f"Config loaded from {args.config}")
    info(f"Mappings loaded from {args.mappings} ({len(pairs)} source→target pairs)")
    info(f"Target columns discovered: {len(targets)}")

    db_path = Path(cfg.get("target_db", "target/warehouse.sqlite"))
    conn = connect_db(db_path, wal=bool(cfg.get("wal", True)), pragma=cfg.get("pragma", {}))
    ensure_raw_events(conn)
    added = widen_table(conn, targets)

    if added:
        info(f"Widened raw_events with {len(added)} columns: {', '.join(added)}")
    else:
        info("No widening needed; schema already matches mappings.")

    if args.show_mappings:
        info("Mappings summary (source_column -> target_column):")
        # Print compact list
        uniq = []
        seen = set()
        for s, t in pairs:
            key = f"{s}->{t}"
            if key not in seen:
                seen.add(key); uniq.append(key)
        for line in uniq:
            sys.stdout.write(f"  {line}\n")
        sys.stdout.flush()
        return

    if args.initdb:
        info(f"DB initialized at {db_path}")
        return

    info("Phase B complete. No ingest in this phase. Use --initdb or --show-mappings.")
    conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        die("Interrupted by user", 130)
