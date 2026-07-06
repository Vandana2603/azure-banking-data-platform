"""
test_validation.py
===================
Unit tests for pyspark/transformations/validation.py
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType

from transformations import validation
from utils.exceptions import SchemaValidationError, DataQualityError, ReferentialIntegrityError


def test_validate_schema_passes(spark):
    schema = StructType([StructField("customer_id", StringType(), True)])
    df = spark.createDataFrame([("CUST001",)], schema)
    validation.validate_schema(df, schema)  # should not raise


def test_validate_schema_fails_on_missing_column(spark):
    expected = StructType([
        StructField("customer_id", StringType(), True),
        StructField("full_name", StringType(), True),
    ])
    df = spark.createDataFrame([("CUST001",)], StructType([StructField("customer_id", StringType(), True)]))
    with pytest.raises(SchemaValidationError):
        validation.validate_schema(df, expected)


def test_validate_not_null_raises_over_threshold(spark):
    df = spark.createDataFrame([(None,), (None,), ("x",)], ["col_a"])
    with pytest.raises(DataQualityError):
        validation.validate_not_null(df, ["col_a"], null_threshold_pct=10.0)


def test_validate_id_format(spark):
    df = spark.createDataFrame([("CUST000001",), ("BADID",)], ["customer_id"])
    result = validation.validate_id_format(df, "customer_id", r"^CUST\d{6}$")
    rows = {r["customer_id"]: r["customer_id_is_valid"] for r in result.collect()}
    assert rows["CUST000001"] is True
    assert rows["BADID"] is False


def test_referential_integrity_raise_on_violation(spark):
    df = spark.createDataFrame([("ACC001", "CUST999"), ("ACC002", "CUST001")], ["account_id", "customer_id"])
    ref_df = spark.createDataFrame([("CUST001",)], ["customer_id"])
    with pytest.raises(ReferentialIntegrityError):
        validation.validate_referential_integrity(
            df, ref_df, "customer_id", "customer_id", raise_on_violation=True
        )


def test_duplicate_rate(spark):
    df = spark.createDataFrame([("A",), ("A",), ("B",)], ["id"])
    rate = validation.duplicate_rate(df, ["id"])
    assert rate == pytest.approx(33.333, abs=0.01)
