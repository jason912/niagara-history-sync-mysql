# Niagara History Sync v3.3

Copies historical data from a Niagara N4 station (via oBIX) into MySQL tables.

> **Disclaimer:** This software is provided under the MIT license, "AS IS", without warranty of any kind. The authors are not liable for any data loss, corruption, or production incidents. You are responsible for testing in a staging environment before production use.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Commands Reference](#commands-reference)
- [Incremental Sync](#incremental-sync)
- [Multi-Station Sync](#multi-station-sync)
- [What's new in v3.3](#whats-new-in-v33)
- [MySQL Structure](#mysql-structure)
- [FAQ](#faq)
- [Requirements](#requirements)

---

## Quick Start

```bash
# 1. Install the only dependency
npm install mysql2

# 2. Interactive setup (station address, oBIX credentials, MySQL connection)
node niagara-sync.js init

# 3. Probe the station to discover history folders
node niagara-sync.js probe

# 4. Start sync
node niagara-sync.js sync
```

---

## Commands Reference

### `init` — Initialize configuration

```bash
node niagara-sync.js init
```
Interactive wizard to fill in station and database info, producing `niagara-sync.json`.

One-shot:
```bash
node niagara-sync.js init --pass Obix12345678 --db-pass <your-mysql-password>
```

### `probe` — Discover station history folders

```bash
node niagara-sync.js probe
```
Connects to the station, discovers available history folders, and updates the config.

### `list` — List syncable history points

```bash
node niagara-sync.js list           # list all
node niagara-sync.js list Temp      # filter by name
node niagara-sync.js list CO2       # filter by name
```

### `sync` — Sync data (main command)

```bash
# Full sync (default 4 concurrent workers)
node niagara-sync.js sync

# Sync only points matching a filter
node niagara-sync.js sync --filter CO2

# Custom concurrency (max 8)
node niagara-sync.js sync --parallel 6

# Sequential mode (one point at a time)
node niagara-sync.js sync --serial

# Sync only recent data (since a given time)
node niagara-sync.js sync --since 2026-07-06T11:00:00

# Limit to a time range
node niagara-sync.js sync --since 2026-07-01T00:00:00 --until 2026-07-07T00:00:00

# Preview mode: count records without writing to DB
node niagara-sync.js sync --dry-run

# Machine-readable JSON output
node niagara-sync.js sync --json
```

### `status` — Check sync state

```bash
node niagara-sync.js status
```
Shows how many rows each point has in MySQL, time ranges, and checkpoint info.

### `config` — View current configuration

```bash
node niagara-sync.js config
node niagara-sync.js config --json
```

---

## Incremental Sync

**Yes, this is incremental.** The tool uses a `_sync_state` table in MySQL for checkpoint tracking:

1. First run performs a full sync
2. After each run, the tool saves each point's **last synced timestamp**
3. Subsequent runs only fetch data **newer than the last checkpoint**

This means you can set up a daily cron job and it will only process new records.

---

## Multi-Station Sync

⚠️ The current version supports **one station at a time**. To sync multiple Niagara stations:

### Manual config switching (recommended)

```bash
# Prepare multiple config files
copy niagara-sync.json   niagara-sync.144.json   # station 144 config
copy niagara-sync.json   niagara-sync.146.json   # station 146 config
# Edit each file's source.host/source.user accordingly

# Sync one by one
copy niagara-sync.144.json niagara-sync.json && node niagara-sync.js sync
copy niagara-sync.146.json niagara-sync.json && node niagara-sync.js sync
```

### Batch script

Create `sync-all-stations.bat`:
```bat
@echo off
echo === Syncing Station 144 ===
copy /Y niagara-sync.144.json niagara-sync.json
node niagara-sync.js sync

echo === Syncing Station 146 ===
copy /Y niagara-sync.146.json niagara-sync.json
node niagara-sync.js sync

echo === All done! ===
```

### Data isolation

- **Different databases**: Set `target.database` per station
- **Same database**: The tool creates one table per point. Tables are named by sanitized point names
- **Watch out**: If two stations have identically named points, they'll share the same table. Use separate databases per station to avoid this

---

## What's new in v3.3

**Parallel sync is now default.** Multiple history points are fetched and written concurrently (4 workers by default). Each point uses its own oBIX query stream and MySQL write — independent and non-blocking.

Sequential mode (one point at a time): `node niagara-sync.js sync --serial`

---

## MySQL Structure

Each history point gets its own table (name = sanitized point name).

### Data table

| Column | Type | Notes |
|---|---|---|
| id | BIGINT AUTO_INCREMENT | Primary key |
| ts | DATETIME(3) | Timestamp in Asia/Shanghai, millisecond precision |
| value | DECIMAL(10,3) | Numeric value (3 decimal precision), NULL for non-numeric |
| raw | TEXT | Raw string value for non-numeric points |

### `_sync_state` table

Tracks checkpoint timestamps per point for incremental sync.

---

## FAQ

### Q: Can I sync multiple stations in one command?
No. The current version is single-station per run. See [Multi-Station Sync](#multi-station-sync) for workarounds.

### Q: Is this incremental or full sync?
**Incremental.** Each run only fetches data newer than the last checkpoint. First run is full.

### Q: What's the minimum Node.js version?
**Node.js >= 10.** The code uses async/await, Buffer, and standard ES6 features only.

*(The README previously said >= 12 — that was conservative. 10+ works fine.)*

### Q: What dependencies do I need?
Just one: `npm install mysql2`. The oBIX protocol uses Node.js built-in `http` and `crypto`.

### Q: What if the sync is interrupted?
Just re-run `sync`. It picks up from the `_sync_state` checkpoint and won't re-sync existing data.

### Q: Sync is slow. How can I speed it up?
- Increase concurrency: `--parallel 8`
- Reduce query batch size: lower `sync.limit` in `niagara-sync.json` (default: 100000)
- Narrow time range: use `--since` and `--until`

---

## Requirements

| Dependency | Version | Notes |
|---|---|---|
| Node.js | >= 10 | Built-in modules only (http, crypto, fs, path) |
| mysql2 | latest | `npm install mysql2` — the only external package |

No other dependencies. No database drivers, build tools, or runtime libraries needed.
