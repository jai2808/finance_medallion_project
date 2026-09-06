# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Orchestrator Driver
# MAGIC Runs Bronze -> Silver -> Gold in sequence for Community Edition.

# COMMAND ----------
import uuid

dbutils.widgets.text("database", "finance_project")
database = dbutils.widgets.get("database")
dbutils.widgets.text("run_id", "")
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

# COMMAND ----------
# MAGIC %run ../utils/audit_logger

# COMMAND ----------
from datetime import datetime
started_ts = datetime.now()
log_pipeline_run(database, run_id, "finance_medallion_pipeline", "STARTED", started_ts)

# COMMAND ----------
# MAGIC %run ./03_bronze_generic_loader

# COMMAND ----------
print("=== STEP 2/3: SILVER ===")
# MAGIC %run ./04_silver_generic_transform

# COMMAND ----------
print("=== STEP 3/3: GOLD ===")
# MAGIC %run ./05_gold_generic_aggregator

# COMMAND ----------
from pyspark.sql import functions as F
print("Full medallion pipeline run complete for database:", database)
log_pipeline_run(database, run_id, "finance_medallion_pipeline", "SUCCESS", started_ts, datetime.now())
display(spark.table(f"{database}_control_audit_log").orderBy(F.col("run_ts").desc()))
