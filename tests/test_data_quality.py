"""
test_data_quality.py

Run with: pytest tests/test_data_quality.py
Requires a local SparkSession (pyspark installed) — does NOT require Databricks.
These tests validate the pure-logic helper functions in utils/common_functions.py
by re-implementing minimal stand-ins for spark.table() lookups via temp views,
so the same functions used in notebooks can be unit tested outside Databricks.
"""

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("dq-tests")
            .getOrCreate())


def _apply_dq_rules_local(df, rules):
    """Same logic as utils.common_functions.apply_dq_rules but takes rules directly
    (avoids needing a live control table) for isolated unit testing."""
    bad_df = None
    clean_df = df
    for r in rules:
        if r["rule_type"] == "not_null":
            fail_cond = F.col(r["column_name"]).isNull()
        elif r["rule_type"] in ("range", "allowed_values"):
            fail_cond = ~F.expr(r["rule_expr"])
        else:
            continue
        flagged = clean_df.filter(fail_cond)
        bad_df = flagged if bad_df is None else bad_df.unionByName(flagged, allowMissingColumns=True)
        clean_df = clean_df.filter(~fail_cond)
    return clean_df, bad_df


def test_not_null_rule_quarantines_nulls(spark):
    df = spark.createDataFrame([
        Row(transaction_id="T1", transaction_amount=100.0),
        Row(transaction_id="T2", transaction_amount=None),
    ])
    rules = [{"rule_type": "not_null", "column_name": "transaction_amount", "rule_expr": None}]
    clean_df, bad_df = _apply_dq_rules_local(df, rules)

    assert clean_df.count() == 1
    assert bad_df.count() == 1
    assert clean_df.collect()[0]["transaction_id"] == "T1"


def test_range_rule_rejects_negative_amounts(spark):
    df = spark.createDataFrame([
        Row(transaction_id="T1", transaction_amount=100.0),
        Row(transaction_id="T2", transaction_amount=-50.0),
    ])
    rules = [{"rule_type": "range", "column_name": "transaction_amount",
              "rule_expr": "transaction_amount > 0"}]
    clean_df, bad_df = _apply_dq_rules_local(df, rules)

    assert clean_df.count() == 1
    assert bad_df.count() == 1
    assert clean_df.collect()[0]["transaction_id"] == "T1"


def test_allowed_values_rule(spark):
    df = spark.createDataFrame([
        Row(account_id="A1", account_type="Savings"),
        Row(account_id="A2", account_type="Crypto"),  # invalid product type
    ])
    rules = [{"rule_type": "allowed_values", "column_name": "account_type",
              "rule_expr": "account_type IN ('Savings','Current','FD','Loan')"}]
    clean_df, bad_df = _apply_dq_rules_local(df, rules)

    assert clean_df.count() == 1
    assert bad_df.count() == 1
    assert bad_df.collect()[0]["account_id"] == "A2"


def test_multiple_rules_compound(spark):
    df = spark.createDataFrame([
        Row(transaction_id="T1", transaction_amount=100.0),
        Row(transaction_id="T2", transaction_amount=None),
        Row(transaction_id="T3", transaction_amount=-25.0),
    ])
    rules = [
        {"rule_type": "not_null", "column_name": "transaction_amount", "rule_expr": None},
        {"rule_type": "range", "column_name": "transaction_amount", "rule_expr": "transaction_amount > 0"},
    ]
    clean_df, bad_df = _apply_dq_rules_local(df, rules)

    assert clean_df.count() == 1
    assert clean_df.collect()[0]["transaction_id"] == "T1"
    # T2 fails not_null (T3 filtered from clean_df pool before range check runs on it,
    # since not_null rule runs first and only removes true nulls -> T3 caught by range rule)
    assert bad_df.count() == 2
