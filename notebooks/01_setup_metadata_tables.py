# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup Metadata Control Tables
# MAGIC Loads the JSON files under `/config` into Delta control tables.
# MAGIC These tables are what every downstream notebook reads to decide *what* to do
# MAGIC and *how* to do it. Re-run this notebook any time a config JSON file changes.

# COMMAND ----------
dbutils.widgets.text("database", "finance_project")
dbutils.widgets.text("config_repo_path", "/dbfs/FileStore/finance_medallion_project/config")
database = dbutils.widgets.get("database")
config_path = dbutils.widgets.get("config_repo_path")

# COMMAND ----------
import json
from pyspark.sql import functions as F

def load_json_config_to_table(file_name, target_table):
    full_path = f"{config_path}/{file_name}"
    with open(full_path, "r") as f:
        data = json.loads(json.dumps(json.load(f)).replace("${database}", database))
    json_rows = [json.dumps(row) for row in data]
    df = spark.read.json(spark.sparkContext.parallelize(json_rows))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f"✅ Loaded {file_name} -> {target_table} ({df.count()} rows)")
    return df

# COMMAND ----------
# MAGIC %md ### Load each control table

# COMMAND ----------
pipeline_config_df = load_json_config_to_table(
    "pipeline_config.json", f"{database}_control_pipeline_config")

column_mapping_df = load_json_config_to_table(
    "column_mapping.json", f"{database}_control_column_mapping")

dq_rules_df = load_json_config_to_table(
    "dq_rules.json", f"{database}_control_data_quality_rules")

gold_config_df = load_json_config_to_table(
    "gold_config.json", f"{database}_control_gold_config")

transformation_rules_df = load_json_config_to_table(
    "transformation_rules.json", f"{database}_control_transformation_rules")

# COMMAND ----------
# Audit log table (created empty, appended to by every pipeline run)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database}_control_audit_log (
    run_id STRING,
    layer STRING,
    source_name STRING,
    status STRING,
    rows_processed BIGINT,
    error_msg STRING,
    run_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database}_control_pipeline_runs (
    run_id STRING,
    pipeline_name STRING,
    status STRING,
    started_ts TIMESTAMP,
    ended_ts TIMESTAMP,
    error_msg STRING
) USING DELTA
""")

print("✅ All metadata control tables are ready.")
display(spark.table(f"{database}_control_pipeline_config"))
