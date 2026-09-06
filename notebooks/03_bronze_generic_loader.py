# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronze Generic Loader
# MAGIC ONE notebook loads EVERY source. Behavior for each source (path, format,
# MAGIC target table) comes entirely from the `pipeline_config` control table —
# MAGIC no source-specific code here. Uses Autoloader (`cloudFiles`) for
# MAGIC schema inference/evolution and exactly-once, checkpointed ingestion.

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

def load_bronze(source_row, catalog):
    source_name = source_row["source_name"]
    fmt = source_row["source_format"]
    path = source_row["source_path"]
    bronze_table = source_row["bronze_table"]
    checkpoint = f"/Volumes/{catalog}/control/checkpoints/{source_name}_bronze/"

    try:
        reader = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", fmt)
                  .option("cloudFiles.schemaLocation", checkpoint + "schema/")
                  .option("cloudFiles.inferColumnTypes", "true")
                  .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))

        df = reader.load(path)

        df_audit = (df
            .withColumn("_ingest_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_source_name", F.lit(source_name))
            .withColumn("_batch_id", F.lit(run_id)))

        query = (df_audit.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(bronze_table))

        query.awaitTermination()

        row_count = spark.table(bronze_table).count()
        log_audit(catalog, "bronze", source_name, "SUCCESS", row_count, None, run_id)
        print(f"✅ Bronze loaded: {bronze_table} (total rows now: {row_count})")

    except Exception as e:
        log_audit(catalog, "bronze", source_name, "FAILED", 0, str(e), run_id)
        print(f"❌ Bronze FAILED for {source_name}: {e}")
        raise

# COMMAND ----------
# MAGIC %md ### Driver loop — reads active sources from metadata and loads each one

# COMMAND ----------
active_sources = spark.table(f"{catalog}.control.pipeline_config") \
    .filter("is_active = true").collect()

print(f"Found {len(active_sources)} active source(s) to load into Bronze.")

for row in active_sources:
    load_bronze(row, catalog)

print("✅ Bronze layer run complete.")
