# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold Generic Aggregator
# MAGIC ONE notebook builds EVERY Gold table (dimensions, facts, KPIs). Each Gold
# MAGIC table is just a row in `gold_config` containing a `CREATE OR REPLACE TABLE ...`
# MAGIC SQL statement. Adding a new KPI = adding a row to `gold_config.json`,
# MAGIC not writing new code. Dimensions/facts are executed before KPIs so
# MAGIC downstream aggregates can safely join against them.

# COMMAND ----------
dbutils.widgets.text("database", "finance_project")
database = dbutils.widgets.get("database")

# COMMAND ----------
# MAGIC %run ../utils/audit_logger

# COMMAND ----------
import uuid
run_id = str(uuid.uuid4())

gold_config = spark.table(f"{database}_control_gold_config") \
    .filter("is_active = true").collect()

# Order matters: dimension -> fact -> kpi
order = {"dimension": 0, "fact": 1, "kpi": 2}
gold_config_sorted = sorted(gold_config, key=lambda r: order.get(r["table_type"], 99))

for row in gold_config_sorted:
    gold_table = row["gold_table"]
    try:
        spark.sql(row["sql_text"])
        row_count = spark.table(gold_table).count()
        log_audit(database, "gold", gold_table, "SUCCESS", row_count, None, run_id)
        print(f"✅ Gold built: {gold_table} ({row['table_type']}, {row_count} rows)")
    except Exception as e:
        log_audit(database, "gold", gold_table, "FAILED", 0, str(e), run_id)
        print(f"❌ Gold FAILED for {gold_table}: {e}")
        raise

print("✅ Gold layer run complete.")

# COMMAND ----------
# MAGIC %md ### Quick preview of a KPI table

# COMMAND ----------
display(spark.table(f"{database}_gold_kpi_monthly_segment_flow").orderBy("txn_month"))
