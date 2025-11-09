# csv2db — CSV → SQLite consolidator (monolithic)
Tag: 251109.180700

## Purpose
Stream many CSV files into one wide SQLite table `raw_events`, storing all mapped fields as TEXT. No transformations. Missing values → ''.
General-purpose. No CPU/RAM/SWAP logic. No QA.

## Folder layout
- source/               # input CSVs
- target/warehouse.sqlite
- target/exports/       # reserved for future SQL-driven exports
- config/app.config.json
- config/mappings.config.csv   # source_column,target_column
- logs/                 # (reserved for future app-level logs; console shows progress)
- qa/                   # unused

## Mappings
Format: source_column,target_column  
Applies to all files. Table widens automatically when new target_column names appear.

Example:
CPU_HOSTNAME,CPU_HOSTNAME
CPU_SAMPLE_DATE,CPU_SAMPLE_DATE
CPU_AVG,CPU_AVG

## Hashing & dedupe
- raw_hash: sha1 over ALL columns in a row (order-insensitive)
- map_hash: sha1 over only mapped target columns
Unique index on raw_hash prevents duplicate inserts on replay.

## Performance defaults
- WAL journaling, synchronous=NORMAL, in-memory temp, 256 MB mmap and cache
- Batched inserts. Autotunes between 10k–200k rows/commit.

## Commands
- Initialize DB + schema:
  python .\csv2db.py --initdb
- Show mappings:
  python .\csv2db.py --show-mappings
- Test on first file without writing:
  python .\csv2db.py --test --sample 12
- Load all CSVs under source/:
  python .\csv2db.py --load
- Overrides:
  python .\csv2db.py --source .\source --pattern "*.csv"

## Config (config/app.config.json)
- pattern: file glob, default "*.csv"
- source_dir: source folder
- target_db: sqlite path
- batch_rows + autotune bounds
- wal + pragma block for tuning

## Notes
- All values ingested as TEXT. No date parsing.
- Hostname remaps or normalization belong in SQL later.
- Keep large CSVs and sqlite files out of Git. See .gitignore.
