# Databricks notebook source
# MAGIC %md
# MAGIC ### common_functions
# MAGIC Meant to be pulled in via `%run ../utils/common_functions`.
# MAGIC Contains all metadata-lookup and transform helpers shared by the
# MAGIC Silver (and optionally Gold) notebooks.

# COMMAND ----------
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def get_column_map(spark, catalog, source_name):
    """Returns the list of column-mapping rows for one source, or [] if none defined."""
    return (spark.table(f"{catalog}.control.column_mapping")
            .filter(f"source_name = '{source_name}'")
            .collect())


def get_dq_rules(spark, catalog, source_name):
    """Returns the list of data-quality rule rows for one source, or [] if none defined."""
    return (spark.table(f"{catalog}.control.data_quality_rules")
            .filter(f"source_name = '{source_name}'")
            .collect())


def apply_column_mapping(spark, catalog, df, source_name):
    """
    Renames + casts columns per the `column_mapping` control table.
    If no mapping rows exist for this source, the dataframe passes through unchanged
    (so brand-new sources still flow to Silver even before mapping is defined).
    Audit columns (prefixed with '_') are always preserved.
    """
    mappings = get_column_map(spark, catalog, source_name)
    if not mappings:
        return df

    select_exprs = [
        F.col(m["src_col"]).cast(m["data_type"]).alias(m["tgt_col"])
        for m in mappings
    ]
    audit_cols = [F.col(c) for c in df.columns if c.startswith("_")]
    return df.select(*select_exprs, *audit_cols)


def apply_dq_rules(spark, catalog, df, source_name):
    """
    Applies every DQ rule defined for this source. Returns (clean_df, bad_df).
    Rows failing ANY rule are removed from clean_df and unioned into bad_df,
    tagged with which rule they failed.
    Supported rule_type values: not_null, range, allowed_values.
    """
    rules = get_dq_rules(spark, catalog, source_name)
    bad_df = None
    clean_df = df

    for r in rules:
        rule_type = r["rule_type"]
        col_name = r["column_name"]

        if rule_type == "not_null":
            fail_cond = F.col(col_name).isNull()
        elif rule_type in ("range", "allowed_values"):
            # rule_expr is a boolean SQL expression describing the VALID condition
            fail_cond = ~F.expr(r["rule_expr"])
        else:
            continue  # unknown rule type -> skip safely

        flagged = clean_df.filter(fail_cond).withColumn(
            "_failed_rule", F.lit(f"{rule_type}:{col_name}"))

        bad_df = flagged if bad_df is None else bad_df.unionByName(
            flagged, allowMissingColumns=True)

        clean_df = clean_df.filter(~fail_cond)

    return clean_df, bad_df


def merge_to_silver(spark, df, silver_table, primary_key):
    """
    Upserts df into silver_table on primary_key.
    Creates the table on first run; MERGEs (update+insert) on subsequent runs.
    """
    if not spark.catalog.tableExists(silver_table):
        df.write.format("delta").saveAsTable(silver_table)
        return

    target = DeltaTable.forName(spark, silver_table)
    (target.alias("t")
        .merge(df.alias("s"), f"t.{primary_key} = s.{primary_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
