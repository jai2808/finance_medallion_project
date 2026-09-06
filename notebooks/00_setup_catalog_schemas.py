# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup Community Edition Database
# MAGIC Creates the default-metastore database and FileStore folders used by the other notebooks.

# COMMAND ----------
dbutils.widgets.text("database", "finance_project")
database = dbutils.widgets.get("database")

# COMMAND ----------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

# COMMAND ----------
# Source-specific landing folders are created by the inbound file drop or demo notebook.
dbutils.fs.mkdirs(f"/FileStore/{database}/landing/")

print("Community Edition database and landing folders created:", database)
