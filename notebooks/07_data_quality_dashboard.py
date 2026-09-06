# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Data Quality & Audit Dashboard
# MAGIC Quick health check notebook: summarizes recent pipeline runs from
# MAGIC `audit_log` and shows how many rows were quarantined per source.
# MAGIC Point a Databricks SQL dashboard or Lakeview dashboard at these same
# MAGIC queries for a persistent view.

# COMMAND ----------
dbutils.widgets.text("database", "finance_project")
database = dbutils.widgets.get("database")

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ### Latest run status per layer/source

# COMMAND ----------
audit_df = spark.table(f"{database}_control_audit_log")
display(audit_df.orderBy(F.col("run_ts").desc()).limit(50))

# COMMAND ----------
# MAGIC %md ### Failure count in the last 7 days

# COMMAND ----------
failures = (audit_df
    .filter(F.col("run_ts") >= F.date_sub(F.current_timestamp(), 7))
    .filter("status = 'FAILED'")
    .groupBy("layer", "source_name")
    .count()
    .orderBy(F.desc("count")))
display(failures)

# COMMAND ----------
# MAGIC %md ### Quarantine volume per source

# COMMAND ----------
quarantine_tables = [t.name for t in spark.catalog.listTables(database)
                      if t.name.startswith(f"{database}_silver_") and t.name.endswith("_quarantine")]

results = []
for t in quarantine_tables:
    cnt = spark.table(t).count()
    results.append((t, cnt))

if results:
    display(spark.createDataFrame(results, ["quarantine_table", "row_count"]))
else:
    print("✅ No quarantine tables found — no rejected records yet.")

# COMMAND ----------
# MAGIC %md ### Row counts across the medallion for sanity-checking lineage

# COMMAND ----------
sources = (spark.table(f"{database}_control_pipeline_config")
           .filter("is_active = true")
           .select("source_name")
           .collect())
for source_row in sources:
    source = source_row["source_name"]
    for layer in ["bronze", "silver"]:
        try:
            cnt = spark.table(f"{database}_{layer}_{source}").count()
            print(f"{layer:7s}.{source:14s} -> {cnt} rows")
        except Exception:
            print(f"{layer:7s}.{source:14s} -> table not found")
