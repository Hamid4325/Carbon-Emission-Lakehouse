# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze to Silver (fixed)
# MAGIC Cleansing, modeling and idempotent upsert into Silver. Primary key: (city, reading_time).

# COMMAND ----------

# 0. Setup and configuration
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.bronze_layer")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.silver_layer")

from functools import reduce
from operator import or_
from datetime import datetime, date, timedelta
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

CATALOG_VOLUME_ROOT = "/Volumes/workspace/default"

bronze_delta_path = f"{CATALOG_VOLUME_ROOT}/bronze_layer/bronze_carbon_data"
silver_delta_path = f"{CATALOG_VOLUME_ROOT}/silver_layer/silver_air_quality_readings"
log_table_path    = f"{CATALOG_VOLUME_ROOT}/bronze_layer/pipeline_execution_logs"  # one audit trail for both layers

# DATA QUALITY POLICY
#   False -> invalid VALUES are set to NULL, the rest of the hour is kept (default)
#   True  -> any hour containing an invalid value is dropped entirely
DROP_INVALID_ROWS = False

print("bronze_delta_path:", bronze_delta_path)
print("silver_delta_path:", silver_delta_path)
print("log_table_path   :", log_table_path)

# COMMAND ----------

# 1. Silver data model (data dictionary). Primary key: (city, reading_time)
silver_schema = StructType([
    StructField("city", StringType(), False),
    StructField("reading_time", TimestampType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("elevation", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("carbon_monoxide_ugm3", DoubleType(), True),
    StructField("carbon_dioxide_ppm", DoubleType(), True),
    StructField("pm2_5_ugm3", DoubleType(), True),
    StructField("pm10_ugm3", DoubleType(), True),
    StructField("nitrogen_dioxide_ugm3", DoubleType(), True),
    StructField("source_file", StringType(), True),
    StructField("bronze_load_timestamp", TimestampType(), True),
    StructField("silver_load_timestamp", TimestampType(), False),
])

# Same schema as the Bronze notebook's log (both layers append to one table)
log_schema = StructType([
    StructField("layer_processed", StringType(), True),
    StructField("parameter_processed", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("rows_processed", IntegerType(), True)
])

# Create the empty Delta tables once so MERGE and logging have a target (safe to re-run)
if DeltaTable.isDeltaTable(spark, silver_delta_path):
    print("Silver table already exists.")
else:
    spark.createDataFrame([], silver_schema).write.format("delta").save(silver_delta_path)
    print("Silver table created.")

if DeltaTable.isDeltaTable(spark, log_table_path):
    print("Log table already exists.")
else:
    spark.createDataFrame([], log_schema).write.format("delta").save(log_table_path)
    print("Audit log table created.")

# COMMAND ----------

# 2. Transformation: explode -> window -> dedupe -> flag invalid values -> clean

# Bronze field -> Silver column
MEASURES = {
    "carbon_monoxide":  "carbon_monoxide_ugm3",
    "carbon_dioxide":   "carbon_dioxide_ppm",
    "pm2_5":            "pm2_5_ugm3",
    "pm10":             "pm10_ugm3",
    "nitrogen_dioxide": "nitrogen_dioxide_ugm3",
}
MEASURE_COLS = list(MEASURES.values())


def check_units(bronze_df: DataFrame):
    """Warn (not fail) if the API units no longer match the units baked into Silver column names."""
    units = bronze_df.select("hourly_units.*").distinct().collect()
    for r in units:
        d = r.asDict()
        if d.get("carbon_dioxide") not in (None, "ppm"):
            print(f"WARNING: carbon_dioxide unit is '{d['carbon_dioxide']}', expected 'ppm'")
        for k in ("carbon_monoxide", "pm2_5", "pm10", "nitrogen_dioxide"):
            if d.get(k) is not None and "g/m" not in d[k]:
                print(f"WARNING: {k} unit is '{d[k]}', expected a g/m3 unit")


def _is_invalid(colname: str, bronze_name: str):
    # Concentrations cannot be negative (this also covers the -999 sentinel).
    # CO2 in ppm cannot be zero either.
    bad = F.isnan(F.col(colname)) | (F.col(colname) < 0)
    if bronze_name == "carbon_dioxide":
        bad = bad | (F.col(colname) == 0)
    return F.coalesce(bad, F.lit(False))   # NULL stays "not invalid": missing is allowed


def build_flagged_readings(bronze_df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    # 1. Explode the parallel hourly arrays into one row per city-hour and cast
    zipped = F.arrays_zip(
        F.col("hourly.time").alias("time"),
        *[F.col(f"hourly.{b}").alias(b) for b in MEASURES]
    )
    exploded = (bronze_df.select(
            "city", "latitude", "longitude", "elevation", "timezone", "source_file",
            F.col("load_timestamp").alias("bronze_load_timestamp"),
            F.explode(zipped).alias("reading"))
        .select(
            "city", "latitude", "longitude", "elevation", "timezone", "source_file",
            "bronze_load_timestamp",
            F.to_timestamp(F.col("reading.time")).alias("reading_time"),
            *[F.col(f"reading.{b}").cast("double").alias(s) for b, s in MEASURES.items()]))

    # 2. Restrict to the requested READING-date window BEFORE deduping (cheaper, same result,
    #    because reading_time is part of the dedupe key). Rows with a NULL reading_time fall
    #    out here since they cannot belong to any window.
    windowed = exploded.filter(
        (F.col("reading_time") >= F.lit(start_date).cast("date")) &
        (F.col("reading_time") <  F.date_add(F.lit(end_date).cast("date"), 1)))

    # 3. Dedupe: latest Bronze load wins (this is how a revised value overrides an old one);
    #    source_file is a deterministic tiebreaker when load timestamps are equal.
    w = Window.partitionBy("city", "reading_time").orderBy(
        F.col("bronze_load_timestamp").desc(), F.col("source_file").desc())
    deduped = (windowed
               .withColumn("_rn", F.row_number().over(w))
               .filter(F.col("_rn") == 1)
               .drop("_rn"))

    # 4. Flag invalid values (one boolean column per measure + an "any" flag)
    flagged = deduped
    for b, s in MEASURES.items():
        flagged = flagged.withColumn(f"_bad_{s}", _is_invalid(s, b))
    flagged = flagged.withColumn("_any_invalid", reduce(or_, [F.col(f"_bad_{s}") for s in MEASURE_COLS]))
    return flagged


_REQUIRED_MISSING = (F.col("city").isNull() | F.col("latitude").isNull() | F.col("longitude").isNull())


def summarize_quality(flagged: DataFrame) -> dict:
    exprs = [
        F.count(F.lit(1)).alias("total_rows"),
        F.sum(F.when(_REQUIRED_MISSING, 1).otherwise(0)).alias("missing_required"),
        F.sum(F.when(F.col("_any_invalid"), 1).otherwise(0)).alias("rows_with_invalid"),
    ] + [F.sum(F.when(F.col(f"_bad_{s}"), 1).otherwise(0)).alias(f"bad_{s}") for s in MEASURE_COLS]
    r = flagged.agg(*exprs).first().asDict()
    r = {k: int(v or 0) for k, v in r.items()}
    r["nulled_values"] = sum(r[f"bad_{s}"] for s in MEASURE_COLS)
    return r


def apply_quality_rules(flagged: DataFrame, drop_invalid_rows: bool) -> DataFrame:
    df = flagged.filter(~_REQUIRED_MISSING)   # NOT NULL columns in Silver need these
    if drop_invalid_rows:
        df = df.filter(~F.col("_any_invalid"))
    else:
        for s in MEASURE_COLS:
            df = df.withColumn(
                s, F.when(F.col(f"_bad_{s}"), F.lit(None).cast("double")).otherwise(F.col(s)))
    return (df.withColumn("silver_load_timestamp", F.current_timestamp())
              .select(*silver_schema.fieldNames()))

# COMMAND ----------

# 3. Idempotent write: MERGE INTO keyed on (city, reading_time)
# Matched rows are updated ONLY if the incoming Bronze row is newer or any value differs.
# So re-running the same window changes nothing, an older load can never overwrite a newer
# one, and a corrected rule or revised API value still propagates.

_COMPARE_COLS = ["latitude", "longitude", "elevation", "timezone"] + MEASURE_COLS
_CHANGED = (
    "s.bronze_load_timestamp > t.bronze_load_timestamp OR NOT ("
    + " AND ".join(f"s.{c} <=> t.{c}" for c in _COMPARE_COLS) + ")"
)


def _latest_version(path: str) -> int:
    return DeltaTable.forPath(spark, path).history(1).select("version").first()[0]


def upsert_to_silver(final_df: DataFrame, silver_table_path: str) -> dict:
    """Runs the MERGE and returns {'source': n, 'inserted': n, 'updated': n}."""
    version_before = _latest_version(silver_table_path)
    (DeltaTable.forPath(spark, silver_table_path).alias("t")
        .merge(final_df.alias("s"), "t.city = s.city AND t.reading_time = s.reading_time")
        .whenMatchedUpdateAll(condition=_CHANGED)
        .whenNotMatchedInsertAll()
        .execute())

    last = (DeltaTable.forPath(spark, silver_table_path).history(1)
            .select("version", "operationMetrics").first())
    if last["version"] > version_before:   # a commit was written by our merge
        m = last["operationMetrics"]
        return {"source": int(m.get("numSourceRows", 0)),
                "inserted": int(m.get("numTargetRowsInserted", 0)),
                "updated": int(m.get("numTargetRowsUpdated", 0))}
    return {"source": 0, "inserted": 0, "updated": 0}   # nothing to merge, no commit

# COMMAND ----------

# 4. Parameterized Bronze-to-Silver function (with logging)
# Takes a READING-DATE range: the dates the readings fall on, not when they were loaded
# into Bronze. rows_processed = source readings merged (same unit as the Bronze log);
# the status text carries inserted/updated and data-quality counts.

def process_bronze_to_silver(start_date: str, end_date: str,
                             bronze_table_path: str, silver_table_path: str,
                             log_table_path: str,
                             drop_invalid_rows: bool = DROP_INVALID_ROWS,
                             raise_on_failure: bool = False):
    layer_name = "Bronze-to-Silver"
    parameter_label = f"{start_date}_to_{end_date}"
    start_time = datetime.now()
    status = "Failed"
    row_count = 0
    error = None

    print(f"--- Starting {layer_name}: {parameter_label} ---")

    try:
        bronze_df = spark.read.format("delta").load(bronze_table_path)
        check_units(bronze_df)

        flagged = build_flagged_readings(bronze_df, start_date, end_date)
        dq = summarize_quality(flagged)            # one extra pass over a small dataset
        final_df = apply_quality_rules(flagged, drop_invalid_rows)

        result = upsert_to_silver(final_df, silver_table_path)
        row_count = result["source"]

        dropped = dq["missing_required"] + (dq["rows_with_invalid"] if drop_invalid_rows else 0)
        status = (f"Success | inserted={result['inserted']} updated={result['updated']} "
                  f"nulled_values={0 if drop_invalid_rows else dq['nulled_values']} "
                  f"dropped_rows={dropped}")
        print(f"{status} | source_rows={row_count}")

    except Exception as e:
        error = e
        row_count = 0
        print(f"Pipeline Failed: {e}")
        status = f"Failed: {str(e)[:2000]}"

    finally:
        end_time = datetime.now()
        log_entry = spark.createDataFrame(
            [(layer_name, parameter_label, start_time, end_time, status, row_count)],
            schema=log_schema)
        log_entry.write.format("delta").mode("append").save(log_table_path)
        print("Audit log updated.")
        print("---------------------------------")

    if error is not None and raise_on_failure:
        raise error
    return status

# COMMAND ----------

# 5. Parameterized backfill wrapper (Silver)

def backfill_silver(start_date: str, end_date: str,
                    bronze_table_path: str, silver_table_path: str,
                    log_table_path: str, window_days: int = 1):
    """Processes [start_date, end_date] in `window_days`-sized chunks.
    For the daily incremental job, call process_bronze_to_silver directly
    with a single 3-5 day rolling window instead of looping this."""
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    results = []
    while current <= end:
        chunk_end = min(current + timedelta(days=window_days - 1), end)
        status = process_bronze_to_silver(
            current.isoformat(), chunk_end.isoformat(),
            bronze_table_path, silver_table_path, log_table_path)
        results.append((current.isoformat(), chunk_end.isoformat(), status))
        current = chunk_end + timedelta(days=1)
    return results

# COMMAND ----------

# 6. Daily incremental run: rolling 5-day window ending today
today = date.today()
window_start = (today - timedelta(days=5)).isoformat()
window_end = today.isoformat()

process_bronze_to_silver(window_start, window_end,
                         bronze_delta_path, silver_delta_path, log_table_path)

# COMMAND ----------

# 7. Idempotency proof: run the EXACT same window again.
# Expect: identical row count, and the second run reports inserted=0 updated=0.
before = spark.read.format("delta").load(silver_delta_path).count()
process_bronze_to_silver(window_start, window_end,
                         bronze_delta_path, silver_delta_path, log_table_path)
after = spark.read.format("delta").load(silver_delta_path).count()
print(f"Silver row count before second run: {before}, after: {after} (should match)")
assert before == after, "Idempotency check failed: row count changed on re-run"

# COMMAND ----------

# 8. Full 3-year load (run once after Bronze has the full history)
process_bronze_to_silver("2023-10-02", "2026-10-02",
                         bronze_delta_path, silver_delta_path, log_table_path)

# Example chunked backfill instead:
# backfill_silver("2023-10-02", "2026-10-02",
#                 bronze_delta_path, silver_delta_path, log_table_path, window_days=30)

# COMMAND ----------

# 9. Verify: audit log and primary-key uniqueness
display(spark.read.format("delta").load(log_table_path).orderBy(F.col("start_time").desc()))

silver_df = spark.read.format("delta").load(silver_delta_path)
dupes = silver_df.groupBy("city", "reading_time").count().filter("count > 1").count()
print(f"Silver rows: {silver_df.count()} | duplicate (city, reading_time) keys: {dupes} (should be 0)")