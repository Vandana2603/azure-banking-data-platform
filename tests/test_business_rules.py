"""
test_business_rules.py
=======================
Unit tests for pyspark/transformations/business_rules.py
"""

from transformations import business_rules


def test_add_transaction_direction(spark):
    df = spark.createDataFrame(
        [("DEPOSIT",), ("WITHDRAWAL",), ("FEE_DEBIT",), ("INTEREST_CREDIT",)],
        ["transaction_type"],
    )
    result = business_rules.add_transaction_direction(df)
    rows = {r["transaction_type"]: r["transaction_direction"] for r in result.collect()}
    assert rows["DEPOSIT"] == "CREDIT"
    assert rows["WITHDRAWAL"] == "DEBIT"
    assert rows["FEE_DEBIT"] == "DEBIT"
    assert rows["INTEREST_CREDIT"] == "CREDIT"


def test_add_payment_size_bucket(spark):
    df = spark.createDataFrame([(500.0,), (10000.0,), (100000.0,), (300000.0,)], ["amount"])
    result = business_rules.add_payment_size_bucket(df)
    buckets = [r["payment_size_bucket"] for r in result.collect()]
    assert buckets == ["Micro", "Small", "Medium", "Large"]


def test_filter_invalid_transactions(spark):
    df = spark.createDataFrame(
        [(True, True), (True, False), (False, True)],
        ["amount_is_valid", "currency_is_valid"],
    )
    result = business_rules.filter_invalid_transactions(df, ["amount_is_valid", "currency_is_valid"])
    assert result.count() == 1
