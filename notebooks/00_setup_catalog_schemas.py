# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup Catalog, Schemas, Volumes
# MAGIC Creates the Unity Catalog objects used by every other notebook in this project.
# MAGIC Run this ONCE per environment (dev/test/prod) with a different `catalog` widget value.

# COMMAND ----------
dbutils.widgets.text("catalog", "finance_project")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")

for schema in ["control", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Volumes used for landing (raw file drops), checkpoints, and quarantine
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.control.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.control.checkpoints")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.control.quarantine")

# COMMAND ----------
# Landing sub-folders per source (mirrors pipeline_config.json source_path values)
for src in ["customers", "branches", "accounts", "transactions"]:
    dbutils.fs.mkdirs(f"/Volumes/{catalog}/control/landing/{src}/")

print("✅ Catalog, schemas, and volumes created:")
display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
