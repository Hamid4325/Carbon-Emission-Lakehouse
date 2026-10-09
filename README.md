# Global Atmospheric Carbon Monitoring Lakehouse

An automated Medallion Architecture (Bronze → Silver → Gold) pipeline built with PySpark on Databricks. It ingests hourly air-quality **concentrations** for 100 global cities from a public API, cleans them into a typed Silver table, and will serve a Gold star schema to a Power BI / Tableau dashboard.

**Team:** Muhammad Hamid Abad (24L-2534), Abdullah Zia (24L-2507)

---

## 1. Data source

| | |
|---|---|
| **Provider** | [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api) (no API key) |
| **Endpoint** | `https://air-quality-api.open-meteo.com/v1/air-quality` |
| **Coverage** | Global; non-European cities come from CAMS global forecasts, available from August 2022 |
| **Variables** | `carbon_monoxide` (µg/m³), `carbon_dioxide` (ppm), `pm2_5`, `pm10`, `nitrogen_dioxide` (µg/m³) |
| **Grain** | One API response per city, holding parallel hourly arrays |

- **Full load:** 3 years of hourly history for 100 cities (`scripts/fetch_full_load.py`).
- **Incremental load:** one file per day (`incremental_YYYY-MM-DD.json`) holding all 100 cities (`scripts/fetch_incremental_load.py`). Recent values are forecast-model output that later runs can revise, so a window of several days can be re-fetched; Silver `MERGE` then updates only the values that changed.
- The data holds **no PII** (model-grid coordinates and pollutant values only).

## 2. Repository structure

```
.
├── README.md
├── docs/                          # Phase 1 proposal, demo video link
├── sample_data/                   # small real samples (full + incremental)
├── scripts/
│   ├── fetch_full_load.py         # 3-year history, 100 cities -> staging/new
│   └── fetch_incremental_load.py  # N daily files (one per day) -> staging/new
└── notebooks/
    ├── 00_Config_and_Logging.ipynb   # paths, staging layout, audit-log helper (shared via %run)
    ├── 01_Raw_to_Bronze.ipynb        # raw JSON -> Bronze (+ schema-drift quarantine, drift demo)
    └── 02_Bronze_to_Silver.ipynb     # Bronze -> Silver (MERGE, DQ, watermark, proof cells)
```

## 3. Storage layout (Databricks Unity Catalog volumes)

```
/Volumes/workspace/default/
├── staging/
│   ├── new/                       # fetched files NOT yet loaded into Bronze
│   ├── processed/                 # files already loaded (moved here after success)
│   └── pipeline_execution_logs    # one audit table for both layers
├── bronze_layer/
│   ├── bronze_carbon_data         # Bronze Delta table
│   └── bronze_quarantine          # rows that broke the schema
└── silver_layer/
    ├── silver_air_quality_readings
    ├── silver_quarantine          # rows that failed validation
    └── silver_watermark           # how far Silver has processed Bronze
```

## 4. Data models

### 4.1 Bronze: `bronze_carbon_data`
Append-only; **one row per city per source file**, kept in the API's nested shape. Written with an explicit `StructType` schema (no `inferSchema`).
**Primary key:** none enforced (append-only audit log). Logical identity is `(city, source_file, load_timestamp)`. Duplicates are removed in Silver.

| Column | Type | Description |
|---|---|---|
| latitude, longitude | DOUBLE | Model grid-cell coordinates |
| generationtime_ms | DOUBLE | API processing time |
| utc_offset_seconds | INT | Offset of the local timezone |
| timezone, timezone_abbreviation | STRING | Local timezone |
| elevation | DOUBLE | Metres |
| hourly_units | STRUCT | Units per variable (all STRING) |
| hourly | STRUCT | Parallel arrays: `time` ARRAY<STRING>; `carbon_monoxide`, `carbon_dioxide`, `pm2_5`, `pm10`, `nitrogen_dioxide` ARRAY<DOUBLE> |
| city | STRING | City name added at fetch time |
| source_system | STRING | `open-meteo-air-quality-api` |
| **source_file** | STRING | Path the file was read from (the file is then moved to `staging/processed`) |
| **load_timestamp** | TIMESTAMP | When the row was loaded into Bronze (identical for every row of a file) |

### 4.2 Bronze quarantine: `bronze_quarantine`
Rows with a column the schema does not know or a value of the wrong type.

| Column | Type | Description |
|---|---|---|
| city | STRING | |
| source_file | STRING | Origin file |
| rescued_data | STRING | JSON of the fields that did not fit the schema |
| quarantine_reason | STRING | `SCHEMA_DRIFT: unexpected column or data-type mismatch` |
| load_timestamp | TIMESTAMP | When it was quarantined |

### 4.3 Silver: `silver_air_quality_readings`
**One row per city per hour.** **Primary key: `(city, reading_time)`.**

| Column | Type | Nullable | Description |
|---|---|---|---|
| **city** | STRING | No | PK |
| **reading_time** | TIMESTAMP | No | PK; exploded from `hourly.time` |
| latitude, longitude, elevation | DOUBLE | Yes | From the payload |
| timezone | STRING | Yes | |
| carbon_monoxide_ugm3 | DOUBLE | Yes | µg/m³ |
| carbon_dioxide_ppm | DOUBLE | Yes | ppm |
| pm2_5_ugm3, pm10_ugm3, nitrogen_dioxide_ugm3 | DOUBLE | Yes | µg/m³ |
| source_file | STRING | Yes | Lineage back to the raw file |
| bronze_load_timestamp | TIMESTAMP | Yes | `load_timestamp` of the Bronze row used |
| **load_timestamp** | TIMESTAMP | No | When this row was written/last changed in Silver |

### 4.4 Silver quarantine: `silver_quarantine`
Same columns as Silver (all nullable) plus `quarantine_reason` (`INVALID_TIMESTAMP`, `NULL_CITY`, `SENTINEL_VALUE_-999`, `NEGATIVE_OR_ZERO_VALUE`) and `load_timestamp`. Logical key: `(city, reading_time, quarantine_reason)`.

### 4.5 Operational tables

**`pipeline_execution_logs`**: one row per file (Raw-to-Bronze) or per run/chunk (Bronze-to-Silver).

| Column | Type | Description |
|---|---|---|
| layer_processed | STRING | `Raw-to-Bronze` / `Bronze-to-Silver` |
| parameter_processed | STRING | File path, date window, or Bronze timestamp range |
| load_type | STRING | `full` / `incremental` / `backfill` (plus test labels) |
| start_time, end_time | TIMESTAMP | Run start and end |
| status | STRING | `Success`, `Partial Success` (some rows quarantined, the rest loaded) or `Failure` |
| rows_inserted, rows_updated | INT | Bronze: inserted = rows loaded. Silver: from the MERGE metrics |
| rows_quarantined | INT | Rows sent to a quarantine table |
| error_message | STRING | Error text, or the quarantine explanation |

**`silver_watermark`**: `processed_up_to` TIMESTAMP (newest Bronze `load_timestamp` already handled), `updated_at` TIMESTAMP.

## 5. How the pipeline works

- **Schema-on-read:** Bronze reads with explicit `StructType`/`StructField` definitions, never `inferSchema`. Silver casts every field to its final type (`to_timestamp`, `double`).
- **Idempotency:** Silver uses `MERGE INTO` on `(city, reading_time)`. Rows that do not exist are inserted; matched rows are updated **only if a value differs**. Running the same input twice therefore changes nothing in Silver (not even `load_timestamp`).
- **Duplicates:** overlapping Bronze loads can repeat an hour; Silver keeps the most recently ingested row, so a revised forecast value wins.
- **Schema drift:** Bronze reads with `rescuedDataColumn`. A new column or a changed type no longer crashes the batch: conforming rows go to Bronze, the others to `bronze_quarantine`, and the log shows `Partial Success`.
- **Bad records in Silver:** invalid timestamps, `-999` sentinels and negative or zero values go to `silver_quarantine` with a reason; the valid rows of the same batch still load.
- **Incremental Silver:** a watermark stores the newest Bronze load already processed, so a normal run needs no dates and processes only what is new.
- **Backfill:** both steps accept a date range and can replay any past period without code changes.
- **Audit:** every run, success or failure, writes a row to `pipeline_execution_logs`.

## 6. Run guide

**One-time setup:** import the three notebooks into the same workspace folder and run `00_Config_and_Logging` (creates volumes, folders and the log table).


### Daily incremental load
1. Fetch: set `WINDOW_DAYS` (1 = yesterday, 5 = five daily files) in `fetch_incremental_load.py` and run it. Files land in `staging/new`.
2. `01_Raw_to_Bronze`: `mode = incremental`, leave the other widgets empty. Every file in `staging/new` is loaded, then moved to `staging/processed`. A failed file stays in `new`.
3. `02_Bronze_to_Silver`: `mode = incremental`, leave dates empty. Only Bronze rows newer than the watermark are processed.

### Backfill an older period
| Step | Widgets |
|---|---|
| Raw → Bronze (by date) | `mode = backfill`, `start_date = 2026-10-01`, `end_date = 2026-10-05`. Searches `new` **and** `processed`. |
| Raw → Bronze (one file) | `mode = backfill`, `file_name = full_load_last_3_years.json` |
| Bronze → Silver | `mode = backfill`, `start_date`, `end_date` (reading dates), optional `chunk_days` (default 90). Ignores the watermark. |

Re-running a backfill is safe: Bronze appends the raw rows again, and the Silver `MERGE` leaves identical data untouched.

## 7. Verified results (from `pipeline_execution_logs`)

| Scenario | Result |
|---|---|
| Full load into Bronze | 100 rows (1 per city), status Success |
| 9 daily incremental files (2026-09-30 to 2026-10-08) | 100 rows each, status Success |
| First Silver incremental run | 2,647,200 rows inserted (100 cities × 26,472 hours), 0 updated, 0 quarantined |
| Same Silver window run twice | 0 inserted, 0 updated both times, row count unchanged |
| One value changed in Silver, window re-run | 0 inserted, **1 updated**, value restored |
| Schema-drift test (1 valid, 1 new column, 1 changed type) | 1 row to Bronze, 2 to `bronze_quarantine`, status Partial Success |

Notebook outputs are saved in `notebooks/`. The proof cells are at the end of `01_Raw_to_Bronze` (drift demo) and `02_Bronze_to_Silver` (re-run and revised-value proof).

## 8. Security & Compliance

The raw payload contains only model-grid coordinates, elevation, timezone, and pollutant readings — **no PII** (no names, emails, or device/account identifiers), confirmed against the real sample above. See the proposal doc for full justification and the (non-privacy-driven) coordinate-rounding convention used for join-key consistency.

## 9. Planned Dashboard

Built in Power BI / Tableau, answering:
- Which regions show the highest YoY growth in carbon concentrations?
- What are the daily/seasonal fluctuation patterns per metro area?
- How often do cities cross WHO hazard thresholds?

Charts: geospatial heatmap, 3-year time-series with Year→Month→Day drill-down, and KPI cards (current global average CO₂, highest-emission city today, YoY % change).

## 10. Infrastructure & Cost Management

Built on **Databricks Free Edition** (serverless; successor to the retired Community Edition). To stay within free-tier limits: pipeline logic is developed and tested on a 1-month data subset before the full 3-year load runs; Silver/Gold tables are partitioned by year and month; storage goes through Unity Catalog Volumes rather than legacy DBFS mounts.
