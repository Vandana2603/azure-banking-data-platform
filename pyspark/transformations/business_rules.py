"""
business_rules.py
==================
Business-logic transformations: calculated columns, filtering invalid
transactions, and enrichment used when building silver/gold datasets.
"""

from pyspark.sql import DataFrame, functions as F


def filter_invalid_transactions(df: DataFrame, flag_columns: list) -> DataFrame:
    """Keep only rows where every one of the given `*_is_valid` boolean flag
    columns is True. Invalid rows are expected to have already been written
    to a quarantine/rejected path by the caller before this filter runs.
    """
    condition = None
    for c in flag_columns:
        if c in df.columns:
            condition = F.col(c) if condition is None else (condition & F.col(c))
    return df.filter(condition) if condition is not None else df


def add_transaction_direction(df: DataFrame, amount_col: str = "amount") -> DataFrame:
    """Add a calculated column classifying a savings transaction as
    credit or debit for reporting."""
    return df.withColumn(
        "transaction_direction",
        F.when(F.col("transaction_type").isin("DEPOSIT", "INTEREST_CREDIT"), F.lit("CREDIT"))
         .when(F.col("transaction_type").isin("WITHDRAWAL", "FEE_DEBIT"), F.lit("DEBIT"))
         .otherwise(F.lit("UNKNOWN")),
    )


def add_payment_size_bucket(df: DataFrame, amount_col: str = "amount") -> DataFrame:
    """Bucket payments into size tiers - a common reporting requirement."""
    return df.withColumn(
        "payment_size_bucket",
        F.when(F.col(amount_col) < 1000, "Micro")
         .when(F.col(amount_col) < 50000, "Small")
         .when(F.col(amount_col) < 200000, "Medium")
         .otherwise("Large"),
    )


def add_customer_age(df: DataFrame, dob_col: str = "date_of_birth") -> DataFrame:
    """Calculate customer age in years from date_of_birth as of today."""
    return df.withColumn(
        "age",
        F.floor(F.datediff(F.current_date(), F.col(dob_col)) / 365.25).cast("int"),
    )


def add_age_group(df: DataFrame, age_col: str = "age") -> DataFrame:
    return df.withColumn(
        "age_group",
        F.when(F.col(age_col) < 25, "18-24")
         .when(F.col(age_col) < 35, "25-34")
         .when(F.col(age_col) < 50, "35-49")
         .when(F.col(age_col) < 65, "50-64")
         .otherwise("65+"),
    )


def add_account_tenure_days(df: DataFrame, opened_date_col: str = "opened_date") -> DataFrame:
    return df.withColumn(
        "account_tenure_days",
        F.datediff(F.current_date(), F.to_date(F.col(opened_date_col))),
    )


def enrich_with_processing_metadata(df: DataFrame, source_system: str,
                                     batch_id: str) -> DataFrame:
    """Attach lineage / audit metadata columns to every record written
    into bronze or silver - required for the data governance / lineage
    requirements of the platform."""
    return (
        df.withColumn("_source_system", F.lit(source_system))
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_ingested_at", F.current_timestamp())
    )
