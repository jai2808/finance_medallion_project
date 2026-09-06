# Databricks notebook source
# MAGIC %md
# MAGIC Approved extension handlers for transformations that are too complex for one SQL expression.
# MAGIC Add a reviewed function here, register its stable name, and reference that name in metadata.

# COMMAND ----------
from pyspark.sql import functions as F


def add_transaction_risk_band(df, rule):
    """Example governed domain transformation selected by metadata."""
    return df.withColumn(
        rule["target_column"],
        F.when(F.col("transaction_amount") >= F.lit(25000), F.lit("HIGH"))
         .when(F.col("transaction_amount") >= F.lit(5000), F.lit("MEDIUM"))
         .otherwise(F.lit("LOW")))


TRANSFORMATION_HANDLERS = {
    "add_transaction_risk_band": add_transaction_risk_band,
}
