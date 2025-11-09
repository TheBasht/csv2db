#!/usr/bin/env python3
# csv2db.py — Phase C: ingest core. 251109.173900
# Scope: CSV streaming ingest only. Modes: --load, --test. QA/exports later.

from __future__ import annotations
import argparse, csv, hashlib, json, os, re, sqlite3, sys, time, glob
from pathlib import Path
from datetime import datetime, timezone
from itertools import islice

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

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

# -----------------------------
# [SECTION: ARGPARSE]
# -----------------------------
def build_args():
    p = argparse.ArgumentParser(description="CSV → SQLite wide-table consolidator")
    p.add_argument("--config", default="config/app.config.json", help="Path to app.config.json")
    p.add_argument("--mappings", default="config/mappings.config.csv", help="Path to mappings CSV (source_column,target_column)")
    p.add_argument("--initdb", action="store_true", help="Create DB and apply schema only")
    p.add_argument("--show-mappings", action="store_true", help="Print mapping summary and exit")
    # Phase C new flags:
    p.add_argument("--load", action="store_true", help="Ingest CSVs into SQLite")
    p.add_argument("--test", action="store_true", help="Preview first N rows from first matching file without DB writes")
    p.add_argument("--sample", type=int, default=10, help="Rows to preview in --test")
    p.add_argument("--pattern", default=None, help="Override file glob pattern (defaults to config)")
    p.add_argument("--source", default=None, help="Override source directory (defaults to config)")
    return p.parse_args()

# -----------------------------
# [SECTION: CONFIG]
# -----------------------------
def load_config(path: Path) -> dict:
    if not path.exists():
        die(f"Missing config: {path}")
    with path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    # Defaults
    cfg.setdefault("utc", True)
    cfg.setdefault("pattern", "*.csv")
    cfg.setdefault("source_dir", "source")
    cfg.setdefault("target_db", "target/warehouse.sqlite")
    cfg.setdefault("wal", True)
    cfg.setdefault("pragma", {})
    cfg.setdefault("batch_rows", 50000)
    cfg.setdefault("batch_rows_min", 10000)
    cfg.setdefault("batch_rows_max", 200000)
    cfg.setdefault("batch_autotune", True)
    cfg.setdefault("log_flush_interval_ms", 500)
    cfg.setdefault("ingest_ver", f"v1.0.0-{APP_TS}")
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
# [SECTION: HASHING]
# -----------------------------
def stable_kv_string(d: dict[str, str]) -> str:
    """Serialize dict as sorted key=value with \x1f separator for hashing."""
    parts = []
    for k in sorted(d.keys()):
        v = d[k] if d[k] is not None else ""
        parts.append(f"{k}={v}")
    return "\x1f".join(parts)

def compute_hashes(row_dict_all: dict[str, str], mapped_targets: dict[str, str]) -> tuple[str, str]:
    """
    raw_hash: sha1 over ALL CSV columns present in the row (order-insensitive).
    map_hash: sha1 over only mapped target columns (order-insensitive).
    """
    raw_hash = sha1_hex(stable_kv_string(row_dict_all))
    map_hash = sha1_hex(stable_kv_string(mapped_targets)) if mapped_targets else sha1_hex("")
    return raw_hash, map_hash

# -----------------------------
# [SECTION: BATCHING]
# -----------------------------
def insert_batch(conn: sqlite3.Connection, insert_sql: str, rows: list[tuple]):
    cur = conn.cursor()
    cur.executemany(insert_sql, rows)
    conn.commit()

def build_insert_sql(targets: list[str]) -> str:
    cols = ["src_file", "src_line", "raw_hash", "map_hash", "loaded_at_utc", "ingest_ver"] + targets
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)
    return f"INSERT OR IGNORE INTO raw_events ({col_list}) VALUES ({placeholders});"

# -----------------------------
# [SECTION: INGEST_LOOP]
# -----------------------------
def iter_csv_files(source_dir: Path, pattern: str) -> list[Path]:
    candidates = [Path(p) for p in glob.glob(str(source_dir.joinpath(pattern)))]
    return [p for p in candidates if p.is_file()]

def parse_header(fpath: Path) -> list[str]:
    with fpath.open("r", encoding="utf-8-sig", newline="") as fh:
        rdr = csv.reader(fh)
        try:
            header = next(rdr)
        except StopIteration:
            return []
    return [h.strip() for h in header]

def ingest_file(conn: sqlite3.Connection, cfg: dict, fpath: Path, targets: list[str], pairs: list[tuple[str,str]]) -> tuple[int,int,int]:
    """
    Returns: (rows_seen, rows_inserted, rows_deduped)
    """
    # map source->target for quick lookup
    src_to_tgt = dict(pairs)
    tgt_set = set(targets)

    header = parse_header(fpath)
    if not header:
        info(f"Empty or headerless file, skipping: {fpath.name}")
        return (0,0,0)
    source_idx = {h: i for i, h in enumerate(header)}

    inserted = 0
    seen = 0
    deduped = 0

    insert_sql = build_insert_sql(targets)
    batch = []
    batch_size = int(cfg.get("batch_rows", 50000))
    batch_min = int(cfg.get("batch_rows_min", 10000))
    batch_max = int(cfg.get("batch_rows_max", 200000))
    autotune = bool(cfg.get("batch_autotune", True))
    ver = str(cfg.get("ingest_ver"))
    log_every = max(10000, batch_size // 2)

    start_time = time.time()
    last_commit_t = start_time

    with fpath.open("r", encoding="utf-8-sig", newline="") as fh:
        rdr = csv.reader(fh)
        # skip header
        try:
            next(rdr)
        except StopIteration:
            return (0,0,0)

        for line_no, row in enumerate(rdr, start=2):  # 1-based header, so data starts at 2
            seen += 1

            # Build dict of ALL columns from this CSV row
            all_dict: dict[str,str] = {}
            for h, idx in source_idx.items():
                try:
                    v = row[idx]
                except IndexError:
                    v = ""
                v = "" if v is None else str(v)
                all_dict[h] = v

            # Build dict of mapped target values (value from source or '')
            mapped_targets: dict[str,str] = {}
            for src, tgt in pairs:
                v = all_dict.get(src, "")
                mapped_targets[tgt] = "" if v is None else str(v)

            raw_hash, map_hash = compute_hashes(all_dict, mapped_targets)

            # Order parameters: base cols then each target in 'targets'
            params = [
                fpath.name,            # src_file
                line_no,               # src_line
                raw_hash,              # raw_hash
                map_hash,              # map_hash
                utc_now_iso(),         # loaded_at_utc
                ver                    # ingest_ver
            ]
            for tgt in targets:
                params.append(mapped_targets.get(tgt, ""))

            batch.append(tuple(params))

            # Progress log
            if seen % log_every == 0:
                info(f"{fpath.name}: seen={seen} inserted~{inserted} deduped~{deduped} batch={len(batch)}")

            # Commit batch
            if len(batch) >= batch_size:
                t0 = time.time()
                insert_before = inserted
                try:
                    insert_batch(conn, insert_sql, batch)
                except sqlite3.IntegrityError:
                    # Unique violation means dedupe occurred; count after checking affected rows via SELECT
                    pass
                # Count net inserted by comparing raw_hash presence; cheaper: rely on cursor.rowcount via separate execute
                # sqlite3 doesn't give rowcount for executemany reliably, so recompute:
                cur = conn.execute("SELECT changes();")
                delta = cur.fetchone()[0]
                if delta < len(batch):
                    deduped += (len(batch) - delta)
                inserted += delta
                batch.clear()
                t1 = time.time()

                # Autotune
                if autotune:
                    duration = t1 - t0
                    if duration < 1.0 and batch_size < batch_max:
                        batch_size = min(int(batch_size * 1.25), batch_max)
                    elif duration > 3.0 and batch_size > batch_min:
                        batch_size = max(int(batch_size * 0.75), batch_min)

    # Final flush
    if batch:
        t0 = time.time()
        try:
            insert_batch(conn, insert_sql, batch)
        except sqlite3.IntegrityError:
            pass
        cur = conn.execute("SELECT changes();")
        delta = cur.fetchone()[0]
        if delta < len(batch):
            deduped += (len(batch) - delta)
        inserted += delta
        batch.clear()

    return (seen, inserted, deduped)

# -----------------------------
# [SECTION: TEST_MODE]
# -----------------------------
def preview_file(fpath: Path, pairs: list[tuple[str,str]], targets: list[str], sample: int = 10):
    header = parse_header(fpath)
    if not header:
        info(f"Empty or headerless file: {fpath.name}")
        return
    source_idx = {h: i for i, h in enumerate(header)}
    info(f"Preview: {fpath.name} header={len(header)} cols, sample={sample}")

    with fpath.open("r", encoding="utf-8-sig", newline="") as fh:
        rdr = csv.reader(fh)
        try:
            next(rdr)  # header
        except StopIteration:
            return
        for line_no, row in zip(range(2, 2 + sample), rdr):
            # All columns
            all_dict = {}
            for h, idx in source_idx.items():
                v = row[idx] if idx < len(row) else ""
                all_dict[h] = "" if v is None else str(v)
            # Mapped
            mapped_targets = {}
            for src, tgt in pairs:
                mapped_targets[tgt] = all_dict.get(src, "")

            raw_hash, map_hash = compute_hashes(all_dict, mapped_targets)

            # Show one-line summary
            sys.stdout.write(f"  line={line_no} raw_hash={raw_hash[:10]} map_hash={map_hash[:10]} mapped={{")

            # print only first 4 mapped pairs for brevity
            shown = 0
            for tgt in targets:
                if shown >= 4: break
                val = mapped_targets.get(tgt, "")
                sys.stdout.write(f"{tgt}='{val}' ")
                shown += 1
            sys.stdout.write("...}\n")
        sys.stdout.flush()
    # Also show example INSERT signature
    insert_sql = build_insert_sql(targets)
    info("Example INSERT signature:")
    info(insert_sql.replace("VALUES", "VALUES (...)"))

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

    # initdb / show-mappings short-circuits
    if args.show_mappings:
        info("Mappings summary (source_column -> target_column):")
        seen = set()
        for s, t in pairs:
            key = f"{s}->{t}"
            if key not in seen:
                seen.add(key)
                sys.stdout.write(f"  {key}\n")
        sys.stdout.flush()
        return
    if args.initdb:
        info(f"DB initialized at {db_path}")
        return

    # Resolve IO paths
    pattern = args.pattern or cfg.get("pattern", "*.csv")
    source_dir = Path(args.source or cfg.get("source_dir", "source")).resolve()
    files = iter_csv_files(source_dir, pattern)
    if not files:
        die(f"No files matched: {source_dir}\\{pattern}")

    if args.test:
        preview_file(files[0], pairs, targets, sample=int(args.sample))
        info("Test mode done. No DB writes.")
        return

    if args.load:
        total_seen = total_inserted = total_dup = 0
        t0 = time.time()
        for fp in files:
            info(f"Begin file: {fp.name}")
            seen, ins, dup = ingest_file(conn, cfg, fp, targets, pairs)
            total_seen += seen; total_inserted += ins; total_dup += dup
            info(f"End file: {fp.name} seen={seen} inserted={ins} deduped={dup}")
        t1 = time.time()
        info(f"LOAD SUMMARY files={len(files)} seen={total_seen} inserted={total_inserted} deduped={total_dup} elapsed_sec={round(t1-t0,2)}")
        return

    info("Nothing to do. Use --initdb, --show-mappings, --test, or --load.")
    conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        die("Interrupted by user", 130)
