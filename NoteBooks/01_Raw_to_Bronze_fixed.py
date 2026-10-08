# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Raw to Bronze (fixed)
# MAGIC Bronze = append-only audit log of exactly what the API returned. Duplicates are expected here;
# MAGIC idempotency is enforced in Silver via MERGE INTO on (city, reading_time).

# COMMAND ----------

# 0. Setup: volumes FIRST (every path below lives inside them)
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.bronze")        # raw JSON lands here
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.bronze_layer")  # Bronze Delta + audit log
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.silver_layer")  # used by the Silver notebook

CATALOG_VOLUME_ROOT = "/Volumes/workspace/default"

bronze_delta_path = f"{CATALOG_VOLUME_ROOT}/bronze_layer/bronze_carbon_data"
log_table_path    = f"{CATALOG_VOLUME_ROOT}/bronze_layer/pipeline_execution_logs"
raw_folder_path   = f"{CATALOG_VOLUME_ROOT}/bronze"   # where raw JSON files land

print("bronze_delta_path:", bronze_delta_path)
print("log_table_path   :", log_table_path)
print("raw_folder_path  :", raw_folder_path)

# COMMAND ----------

# 1. Strict schema (schema-on-read)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    ArrayType, TimestampType
)
from pyspark.sql import functions as F
from datetime import datetime, date, timedelta
from delta.tables import DeltaTable
import os

hourly_units_schema = StructType([
    StructField("time", StringType(), True),
    StructField("carbon_monoxide", StringType(), True),
    StructField("carbon_dioxide", StringType(), True),
    StructField("pm2_5", StringType(), True),
    StructField("pm10", StringType(), True),
    StructField("nitrogen_dioxide", StringType(), True)
])

hourly_schema = StructType([
    StructField("time", ArrayType(StringType()), True),
    StructField("carbon_monoxide", ArrayType(DoubleType()), True),
    StructField("carbon_dioxide", ArrayType(DoubleType()), True),
    StructField("pm2_5", ArrayType(DoubleType()), True),
    StructField("pm10", ArrayType(DoubleType()), True),
    StructField("nitrogen_dioxide", ArrayType(DoubleType()), True)
])

open_meteo_schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("generationtime_ms", DoubleType(), True),
    StructField("utc_offset_seconds", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_abbreviation", StringType(), True),
    StructField("elevation", DoubleType(), True),
    StructField("hourly_units", hourly_units_schema, True),
    StructField("hourly", hourly_schema, True),
    StructField("city", StringType(), True),
    StructField("source_system", StringType(), True)
])

print("Strict schema defined.")

# COMMAND ----------

# 2. Sanity check: test the schema against a real sample file (FAILFAST = bad records raise, not null out)
sample_file_path = f"{raw_folder_path}/incremental_load_sample.json"

try:
    test_df = (spark.read
               .schema(open_meteo_schema)
               .option("multiline", "true")
               .option("mode", "FAILFAST")
               .json(sample_file_path))
    print("File read successfully! Preview:")
    display(test_df)
except Exception as e:
    print("Error reading the file. Check your file path!")
    print(e)

# COMMAND ----------

# 3. Audit log table (run once; safe to re-run)
# rows_processed = number of hourly READINGS in the file (same unit in Bronze and Silver logs).
log_schema = StructType([
    StructField("layer_processed", StringType(), True),
    StructField("parameter_processed", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("rows_processed", IntegerType(), True)
])

if DeltaTable.isDeltaTable(spark, log_table_path):
    print("Log table already exists.")
else:
    spark.createDataFrame([], log_schema).write.format("delta").save(log_table_path)
    print("Audit log table created successfully.")

# COMMAND ----------

# DESIGN DECISION: Bronze writes with mode="append".
# Re-running on the same file duplicates raw rows in Bronze -- intentional.
# Bronze is an append-only record of what the API returned on every call, including
# overlapping re-fetches. "No duplicates downstream" is guaranteed in Silver via
# MERGE INTO on (city, reading_time), not here.

def ingest_raw_to_bronze(input_file_path, bronze_table_path, log_table_path,
                         raise_on_failure=False):
    layer_name = "Raw-to-Bronze"
    start_time = datetime.now()
    status = "Failed"
    row_count = 0
    error = None

    print(f"--- Starting {layer_name}: {input_file_path} ---")

    try:
        # 1. Read (FAILFAST: malformed records raise instead of silently becoming null rows)
        raw_df = (spark.read
                  .schema(open_meteo_schema)
                  .option("multiline", "true")
                  .option("mode", "FAILFAST")
                  .json(input_file_path))

        bronze_df = (raw_df
                     .withColumn("load_timestamp", F.current_timestamp())
                     .withColumn("source_file", F.col("_metadata.file_path")))

        # Count READINGS (hourly array elements), not top-level JSON records.
        # This is one extra pass over a small file, and it also forces FAILFAST
        # validation BEFORE anything is written.
        row_count = int(
            bronze_df.agg(
                F.coalesce(F.sum(F.greatest(F.size("hourly.time"), F.lit(0))), F.lit(0))
            ).first()[0]
        )

        # 2. Write to Bronze (append-only; no mergeSchema -- the schema is strict on purpose)
        (bronze_df.write
         .format("delta")
         .mode("append")
         .save(bronze_table_path))

        status = "Success"
        print(f"Success! {row_count} hourly readings appended to Bronze layer.")

    except Exception as e:
        error = e
        row_count = 0
        print(f"Pipeline Failed: {e}")
        status = f"Failed: {str(e)[:2000]}"   # generous cap; full message kept for debugging

    finally:
        end_time = datetime.now()
        log_entry = spark.createDataFrame(
            [(layer_name, input_file_path, start_time, end_time, status, row_count)],
            schema=log_schema
        )
        log_entry.write.format("delta").mode("append").save(log_table_path)
        print("Audit log updated.")
        print("---------------------------------")

    # Re-raise AFTER logging so a Databricks Job shows FAILED when ingestion fails
    if error is not None and raise_on_failure:
        raise error

    return status

# COMMAND ----------

# 4. Optional: date-range wrapper.
# ONLY useful if raw files are named raw_folder/YYYY-MM-DD.json (one file per day).
# Your current files (incremental_load_sample.json, full_load_last_3_years.json) do not
# follow that pattern, so this is not called below. Missing days are skipped, not failed.

def backfill_bronze(start_date: str, end_date: str, raw_folder: str,
                    bronze_table_path: str, log_table_path: str):
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    results = []
    while current <= end:
        file_path = f"{raw_folder}/{current.isoformat()}.json"
        if os.path.exists(file_path):
            status = ingest_raw_to_bronze(file_path, bronze_table_path, log_table_path)
        else:
            status = "Skipped: file not found"
        results.append((current.isoformat(), status))
        current += timedelta(days=1)
    return results

# COMMAND ----------

# 5. Run
# --- Single-file run
# source_data_parameter = f"{raw_folder_path}/incremental_load_sample.json"
# ingest_raw_to_bronze(source_data_parameter, bronze_delta_path, log_table_path)

# --- Full load (3 years). NOTE: running this twice appends the data twice (by design).
source_data_parameter = f"{raw_folder_path}/full_load_last_3_years.json"
ingest_raw_to_bronze(source_data_parameter, bronze_delta_path, log_table_path)

display(spark.read.format("delta").load(log_table_path).orderBy(F.col("start_time").desc()))