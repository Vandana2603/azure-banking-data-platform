"""
validation.py
=============
Schema and business-rule validation used to enforce data quality before
records move from bronze -> silver -> gold.
"""

from typing import List, Dict

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType

from pyspark.sql.utils import AnalysisException

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.exceptions import SchemaValidationError, DataQualityError, ReferentialIntegrityError


def validate_schema(df: DataFrame, expected_schema: StructType) -> None:
    """Raise SchemaValidationError if the DataFrame schema does not contain
    all expected columns with compatible types.

    We validate column *presence and type*, not exact ordering, since source
    systems frequently reorder or add columns without breaking contracts.
    """
    actual_fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    missing = []
    mismatched = []

    for field in expected_schema.fields:
        if field.name not in actual_fields:
            missing.append(field.name)
        elif actual_fields[field.name] != field.dataType.simpleString():
            mismatched.append(
                f"{field.name} (expected {field.dataType.simpleString()}, "
                f"got {actual_fields[field.name]})"
            )

    if missing or mismatched:
        raise SchemaValidationError(
            f"Schema validation failed. Missing columns: {missing}. "
            f"Type mismatches: {mismatched}."
        )


def validate_not_null(df: DataFrame, columns: List[str],
                       null_threshold_pct: float = 2.0) -> Dict[str, float]:
    """Check null percentage for critical columns. Raises DataQualityError
    if any column exceeds the allowed null threshold. Returns a report dict
    for logging / data-quality reporting regardless.
    """
    total = df.count()
    report = {}
    violations = []

    if total == 0:
        return report

    for c in columns:
        if c not in df.columns:
            continue
        null_count = df.filter(F.col(c).isNull()).count()
        pct = round((null_count / total) * 100, 3)
        report[c] = pct
        if pct > null_threshold_pct:
            violations.append(f"{c}: {pct}% nulls (threshold {null_threshold_pct}%)")

    if violations:
        raise DataQualityError(f"Null threshold exceeded -> {violations}")

    return report


def validate_id_format(df: DataFrame, column: str, regex: str) -> DataFrame:
    """Flag rows where an ID column doesn't match the expected regex pattern
    (e.g. CUST###### or ACC#######). Adds a boolean `<column>_is_valid` flag
    rather than silently dropping rows, so callers decide how to handle them.
    """
    return df.withColumn(f"{column}_is_valid", F.col(column).rlike(regex))


def validate_referential_integrity(df: DataFrame, ref_df: DataFrame,
                                    key_column: str, ref_key_column: str,
                                    raise_on_violation: bool = False) -> DataFrame:
    """Flag (or optionally raise on) rows whose foreign key does not exist
    in the referenced dimension table. This is the PySpark equivalent of a
    FK constraint check, run before loading into the warehouse.
    """
    ref_keys = ref_df.select(F.col(ref_key_column).alias("_ref_key")).distinct()
    joined = df.join(ref_keys, df[key_column] == ref_keys["_ref_key"], how="left")
    joined = joined.withColumn(f"{key_column}_is_valid", F.col("_ref_key").isNotNull()).drop("_ref_key")

    if raise_on_violation:
        invalid_count = joined.filter(~F.col(f"{key_column}_is_valid")).count()
        if invalid_count > 0:
            raise ReferentialIntegrityError(
                f"{invalid_count} rows have invalid '{key_column}' "
                f"not present in reference table."
            )
    return joined


def validate_amount_range(df: DataFrame, amount_col: str,
                           min_amount: float, max_amount: float) -> DataFrame:
    """Flag transactions whose amount falls outside a sane business range."""
    return df.withColumn(
        f"{amount_col}_is_valid",
        (F.col(amount_col) >= min_amount) & (F.col(amount_col) <= max_amount),
    )


def duplicate_rate(df: DataFrame, key_columns: List[str]) -> float:
    """Return the percentage of duplicate rows based on key_columns."""
    total = df.count()
    if total == 0:
        return 0.0
    distinct = df.dropDuplicates(key_columns).count()
    return round(((total - distinct) / total) * 100, 3)
