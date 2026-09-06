# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Orchestrator Driver
# MAGIC Runs Bronze -> Silver -> Gold in sequence for the given catalog.
# MAGIC This is the single notebook a Databricks Job/Workflow schedules
# MAGIC (see `jobs/workflow_job.json` for the equivalent 3-task job definition,
# MAGIC which is the preferred production approach since it gives per-layer
# MAGIC retries and observability in the Jobs UI).

# COMMAND ----------
dbutils.widgets.text("catalog", "finance_project")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
print("=== STEP 1/3: BRONZE ===")
dbutils.notebook.run("03_bronze_generic_loader", timeout_seconds=3600, arguments={"catalog": catalog})

# COMMAND ----------
print("=== STEP 2/3: SILVER ===")
dbutils.notebook.run("04_silver_generic_transform", timeout_seconds=3600, arguments={"catalog": catalog})

# COMMAND ----------
print("=== STEP 3/3: GOLD ===")
dbutils.notebook.run("05_gold_generic_aggregator", timeout_seconds=3600, arguments={"catalog": catalog})

# COMMAND ----------
print("✅ Full medallion pipeline run complete for catalog:", catalog)
display(spark.table(f"{catalog}.control.audit_log").orderBy(F.col("run_ts").desc()))
