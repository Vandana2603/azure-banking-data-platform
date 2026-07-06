"""
cleaning.py
===========
Reusable, pure PySpark data-cleaning functions used across bronze -> silver
transformations. Every function takes a DataFrame in, returns a DataFrame out,
so they can be chained with `.transform()`.
"""

from typing import List, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Lowercase and snake_case all column names for consistency."""
    for col in df.columns:
        new_col = col.strip().lower().replace(" ", "_")
        if new_col != col:
            df = df.withColumnRenamed(col, new_col)
    return df


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace on every string column."""
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def standardize_text_case(df: DataFrame, columns: List[str], mode: str = "title") -> DataFrame:
    """Standardize casing for the given text columns.

    Args:
        mode: one of "title", "upper", "lower".
    """
    case_fn = {"title": F.initcap, "upper": F.upper, "lower": F.lower}.get(mode, F.initcap)
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, case_fn(F.col(c)))
    return df


def standardize_dates(df: DataFrame, columns: List[str],
                       output_format: str = "yyyy-MM-dd") -> DataFrame:
    """Parse dates that may arrive in several inconsistent formats and
    re-emit them in a single canonical format.

    Uses Spark's `to_timestamp` with a candidate-format cascade via
    `coalesce`, which is far cheaper than a Python UDF.
    """
    candidate_formats = [
        "yyyy-MM-dd",
        "dd/MM/yyyy",
        "MM-dd-yyyy",
        "yyyy/MM/dd HH:mm:ss",
        "dd-MMM-yyyy",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss",
    ]
    for c in columns:
        if c not in df.columns:
            continue
        parsed_candidates = [F.to_timestamp(F.col(c), fmt) for fmt in candidate_formats]
        parsed = F.coalesce(*parsed_candidates)
        df = df.withColumn(f"{c}_parsed", parsed)
        df = df.withColumn(c, F.date_format(F.col(f"{c}_parsed"), output_format))
        df = df.drop(f"{c}_parsed")
    return df


def standardize_currency(df: DataFrame, amount_col: str = "amount",
                          currency_col: str = "currency",
                          allowed_currencies: Optional[List[str]] = None) -> DataFrame:
    """Round monetary amounts to 2 decimal places and flag/normalize
    unexpected currency codes to uppercase 3-letter form.
    """
    allowed_currencies = allowed_currencies or ["INR", "USD", "EUR", "GBP"]
    df = df.withColumn(amount_col, F.round(F.col(amount_col).cast("double"), 2))
    df = df.withColumn(currency_col, F.upper(F.trim(F.col(currency_col))))
    df = df.withColumn(
        f"{currency_col}_is_valid",
        F.col(currency_col).isin(allowed_currencies),
    )
    return df


def deduplicate(df: DataFrame, key_columns: List[str],
                 order_by: Optional[str] = None) -> DataFrame:
    """Remove duplicate records based on business key columns.

    If `order_by` is provided (e.g. 'updated_at'), keeps the most recent
    record per key using a window function instead of a naive dropDuplicates,
    which matters when duplicates have different update timestamps.
    """
    if order_by and order_by in df.columns:
        from pyspark.sql.window import Window
        w = Window.partitionBy(*key_columns).orderBy(F.col(order_by).desc())
        df = (
            df.withColumn("_row_num", F.row_number().over(w))
              .filter(F.col("_row_num") == 1)
              .drop("_row_num")
        )
        return df
    return df.dropDuplicates(key_columns)


def handle_missing_values(df: DataFrame, strategy: dict) -> DataFrame:
    """Apply per-column missing-value strategies.

    Args:
        strategy: dict mapping column_name -> fill value, e.g.
            {"gender": "Unknown", "description": "Not Provided", "balance": 0.0}
    """
    return df.fillna(strategy)


def drop_fully_null_rows(df: DataFrame) -> DataFrame:
    """Drop rows where every column is null (empty/corrupt source rows)."""
    return df.dropna(how="all")
