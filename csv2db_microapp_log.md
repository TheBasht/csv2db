# csv2db Microapp Development Log

## Overview
Conversation summary documenting project setup, structure, and implementation stages for the csv2db microapp.

---

## Key Decisions
- Database: SQLite — sufficient for ≤305 MB of CSV data.
- Path model: Relative, with root `C:\dev\csv2db`.
- Timestamp format: Strict UTC `"YYYY-MM-DDTHH:MM:SS.000+0000"`.
- Deduplication: Global, by SHA-256 hash over all target columns.
- Only mapped columns kept; unmapped dropped.
- Config files are read-only (pipeline never edits).

---

## Folder Structure
```
/a.source/      # raw CSVs
/c.target/      # database and reports
/d.config/      # configuration and mappings
/f.logs/        # run logs
app.py
README.txt
```

---

## Implementation Stages

### Stage 1 — Scaffold
Create base folders and placeholder configs via PowerShell.

### Stage 2 — Mapping Authoring
Manually fill `d.config/mappings.config.csv` with:
```
filename,source_column,target_column,is_date
```

### Stage 3 — Plan Preview
Command: `python app.py --plan`  
- Scans CSVs and mappings  
- Reports row estimates, unmapped columns, missing headers  

### Stage 4 — Load + Normalize
Command: `python app.py --load`  
- Normalizes timestamps to UTC strict format  
- Inserts into SQLite in batches of 5000  
- Dedupes globally via `INSERT OR IGNORE` on `row_hash`  
- Writes summary log and `load_summary.<timestamp>.txt`

### Stage 5 – 6 — QA Checks
Command: `python app.py --qa`  
- Confirms no duplicate `row_hash`  
- Validates timestamp format (`+0000`)  
- Counts empty fields per column  
- Exports `qa_report.<timestamp>.txt`

---

## Deduplication Behavior
- Occurs **during load**.  
- `row_hash` UNIQUE constraint in SQLite ensures no duplicate entries.  
- `--qa` only verifies duplicates, never modifies data.

---

## Typical Run Sequence
```
python app.py --plan
python app.py --load
python app.py --qa
```

---

## Observations
- Load batching: 5 000 rows per commit.  
- Deduplication scope: global across entire table.  
- QA confirmed 0 bad timestamps and expected empty columns.

---

## Current Results
Example outcome:
```
TOTAL rows_seen=1380334 inserted=277 dedup=1380057
QA: rows_total=1380335 distinct_hash=1380335 dup_in_table=0
```
Interpretation: existing 1 380 058 rows retained, 277 new unique rows inserted.

---

## End of Record
All seven stages complete.
