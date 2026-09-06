# Databricks notebook source
# MAGIC %md
# MAGIC ### audit_logger
# MAGIC Meant to be pulled in via `%run ../utils/audit_logger` from any notebook.
# MAGIC Provides `log_audit(...)` which appends one row to `{catalog}.control.audit_log`.

# COMMAND ----------
from pyspark.sql import Row
from datetime import datetime

def log_audit(catalog, layer, source_name, status, rows_processed=0, error_msg=None, run_id=None):
    """
    Appends a single audit record.

    Parameters
    ----------
    catalog        : str   - target catalog, e.g. "finance_project"
    layer          : str   - "bronze" | "silver" | "gold"
    source_name    : str   - source/table name this record pertains to
    status         : str   - "SUCCESS" | "FAILED"
    rows_processed : int   - row count after the operation
    error_msg      : str   - exception message if status == FAILED
    run_id         : str   - shared UUID across all steps of one orchestrator run
    """
    log_row = spark.createDataFrame([Row(
        run_id=run_id,
        layer=layer,
        source_name=source_name,
        status=status,
        rows_processed=rows_processed,
        error_msg=error_msg,
        run_ts=datetime.now()
    )])
    log_row.write.format("delta").mode("append") \
        .saveAsTable(f"{catalog}.control.audit_log")
