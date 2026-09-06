# Databricks notebook source
# MAGIC %md
# MAGIC ### audit_logger
# MAGIC Meant to be pulled in via `%run ../utils/audit_logger` from any notebook.
# MAGIC Provides `log_audit(...)` which appends one row to the Community Edition audit table.

# COMMAND ----------
from pyspark.sql import Row
from datetime import datetime
from delta.tables import DeltaTable


def log_pipeline_run(catalog, run_id, pipeline_name, status, started_ts, ended_ts=None, error_msg=None):
    """Writes an idempotent pipeline-level status record for operational monitoring."""
    run_row = spark.createDataFrame([Row(
        run_id=run_id,
        pipeline_name=pipeline_name,
        status=status,
        started_ts=started_ts,
        ended_ts=ended_ts,
        error_msg=error_msg
    )])
    run_table = f"{catalog}_control_pipeline_runs"
    if not spark.catalog.tableExists(run_table):
        run_row.write.format("delta").saveAsTable(run_table)
        return

    (DeltaTable.forName(spark, run_table).alias("target")
        .merge(run_row.alias("source"), "target.run_id = source.run_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

def log_audit(catalog, layer, source_name, status, rows_processed=0, error_msg=None, run_id=None):
    """
    Appends a single audit record.

    Parameters
    ----------
    catalog        : str   - target Community Edition database, e.g. "finance_project"
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
        .saveAsTable(f"{catalog}_control_audit_log")
