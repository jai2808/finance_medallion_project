# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver Generic Transform
# MAGIC ONE notebook cleanses EVERY source: applies column mapping/casting from
# MAGIC `column_mapping`, applies validation rules from `data_quality_rules`
# MAGIC (bad records go to a `<source>_quarantine` table), deduplicates on the
# MAGIC primary key, and MERGEs the clean result into the Silver Delta table.

# COMMAND ----------
dbutils.widgets.text("catalog", "finance_project")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------
# MAGIC %run ../utils/common_functions

# COMMAND ----------
# MAGIC %run ../utils/audit_logger

# COMMAND ----------
from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())

active_sources = spark.table(f"{catalog}.control.pipeline_config") \
    .filter("is_active = true").collect()

for row in active_sources:
    source_name = row["source_name"]
    bronze_table = row["bronze_table"]
    silver_table = row["silver_table"]
    primary_key = row["primary_key"]

    try:
        bronze_df = spark.table(bronze_table)

        # 1) Apply column mapping (rename + cast); pass-through if no mapping defined
        mapped_df = apply_column_mapping(spark, catalog, bronze_df, source_name)

        # 2) Apply data quality rules -> split into clean / quarantine
        clean_df, bad_df = apply_dq_rules(spark, catalog, mapped_df, source_name)

        # 3) Deduplicate on primary key (keep latest by ingest timestamp)
        if "_ingest_timestamp" in clean_df.columns:
            from pyspark.sql import Window
            w = Window.partitionBy(primary_key).orderBy(F.col("_ingest_timestamp").desc())
            clean_df = (clean_df.withColumn("_rn", F.row_number().over(w))
                        .filter("_rn = 1").drop("_rn"))
        else:
            clean_df = clean_df.dropDuplicates([primary_key])

        clean_df = clean_df.withColumn("_silver_load_ts", F.current_timestamp())

        # 4) MERGE into Silver (upsert)
        merge_to_silver(spark, clean_df, silver_table, primary_key)

        # 5) Write rejected/quarantined rows for investigation
        if bad_df is not None and bad_df.count() > 0:
            quarantine_table = f"{catalog}.silver.{source_name}_quarantine"
            bad_df.withColumn("_quarantined_ts", F.current_timestamp()) \
                  .write.format("delta").mode("append").saveAsTable(quarantine_table)
            print(f"⚠️  {bad_df.count()} rows quarantined -> {quarantine_table}")

        clean_count = clean_df.count()
        log_audit(catalog, "silver", source_name, "SUCCESS", clean_count, None, run_id)
        print(f"✅ Silver loaded: {silver_table} ({clean_count} clean rows merged)")

    except Exception as e:
        log_audit(catalog, "silver", source_name, "FAILED", 0, str(e), run_id)
        print(f"❌ Silver FAILED for {source_name}: {e}")
        raise

print("✅ Silver layer run complete.")
