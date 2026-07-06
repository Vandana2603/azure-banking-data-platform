"""
test_cleaning.py
=================
Unit tests for pyspark/transformations/cleaning.py
"""

from transformations import cleaning


def test_standardize_column_names(spark):
    df = spark.createDataFrame([(1, "a")], ["Customer ID", "Full Name"])
    result = cleaning.standardize_column_names(df)
    assert result.columns == ["customer_id", "full_name"]


def test_trim_string_columns(spark):
    df = spark.createDataFrame([("  Alice  ", 30)], ["name", "age"])
    result = cleaning.trim_string_columns(df)
    assert result.collect()[0]["name"] == "Alice"


def test_deduplicate_keeps_latest(spark):
    df = spark.createDataFrame(
        [
            ("CUST001", "2024-01-01"),
            ("CUST001", "2024-06-01"),
            ("CUST002", "2024-02-01"),
        ],
        ["customer_id", "updated_at"],
    )
    result = cleaning.deduplicate(df, ["customer_id"], order_by="updated_at")
    assert result.count() == 2
    latest = result.filter("customer_id = 'CUST001'").collect()[0]
    assert latest["updated_at"] == "2024-06-01"


def test_standardize_currency_flags_invalid(spark):
    df = spark.createDataFrame(
        [(100.567, "inr"), (50.0, "xyz")], ["amount", "currency"]
    )
    result = cleaning.standardize_currency(df, "amount", "currency", ["INR", "USD"])
    rows = {r["currency"]: r["currency_is_valid"] for r in result.collect()}
    assert rows["INR"] is True
    assert rows["XYZ"] is False


def test_handle_missing_values(spark):
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
    ])
    df = spark.createDataFrame([("Alice", None)], schema)
    result = cleaning.handle_missing_values(df, {"gender": "Unknown"})
    assert result.collect()[0]["gender"] == "Unknown"
