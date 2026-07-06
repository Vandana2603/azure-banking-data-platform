"""
bronze_to_silver.py
====================
Databricks job / spark-submit script.

Reads raw CSV/JSON banking files (bronze/raw layer), applies cleaning,
validation and business rules, and writes curated Parquet/Delta datasets
to the silver layer.

Run locally:
    spark-submit pyspark/jobs/bronze_to_silver.py --config config/config.yaml

Run on Databricks:
    Import as a notebook (see notebooks/01_bronze_to_silver.py) or schedule
    as a Databricks Job pointing at this file, triggered from Azure Data
    Factory via a "Databricks Notebook" / "Databricks Python" activity.
"""

import argparse
import os
import sys
import uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

from transformations import cleaning, validation, business_rules
from utils.config_loader import load_config
from utils.logger import get_logger
from utils.exceptions import PipelineError

logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Expected schemas (contracts) for each raw dataset
# --------------------------------------------------------------------------- #

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("kyc_status", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

ACCOUNTS_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("branch_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("opened_date", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("updated_at", StringType(), True),
])


def get_spark(app_name: str = "BankBronzeToSilver") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")          # AQE - perf optimization
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def process_customers(spark: SparkSession, cfg, raw_path: str, silver_path: str, batch_id: str):
    logger.info("Processing customers: %s", raw_path)
    df = spark.read.option("header", True).csv(raw_path)

    validation.validate_schema(df.select(
        [f.name for f in CUSTOMERS_SCHEMA.fields if f.name in df.columns]
    ), StructType([f for f in CUSTOMERS_SCHEMA.fields if f.name in df.columns]))

    df = (
        df.transform(cleaning.standardize_column_names)
          .transform(cleaning.trim_string_columns)
          .transform(lambda d: cleaning.standardize_dates(d, ["date_of_birth", "created_at", "updated_at"]))
          .transform(lambda d: cleaning.standardize_text_case(d, ["full_name", "city", "state"], "title"))
          .transform(lambda d: cleaning.handle_missing_values(
              d, {"gender": "Unknown", "email": "not_provided@unknown.com"}))
          .transform(lambda d: cleaning.deduplicate(d, ["customer_id"], order_by="updated_at"))
          .transform(lambda d: validation.validate_id_format(d, "customer_id", r"^CUST\d{6}$"))
          .transform(lambda d: business_rules.add_customer_age(d, "date_of_birth"))
          .transform(business_rules.add_age_group)
          .transform(lambda d: business_rules.enrich_with_processing_metadata(d, "core_banking_csv", batch_id))
    )

    null_report = validation.validate_not_null(
        df, ["customer_id", "full_name"],
        null_threshold_pct=cfg.get("data_quality", "null_threshold_pct", default=2.0),
    )
    logger.info("Customer null report: %s", null_report)

    valid_df = df.filter("customer_id_is_valid = true")
    invalid_df = df.filter("customer_id_is_valid = false")

    valid_df.write.mode("overwrite").format("parquet").save(f"{silver_path}/customers")
    if invalid_df.count() > 0:
        invalid_df.write.mode("overwrite").format("parquet").save(f"{silver_path}/_rejected/customers")
        logger.warning("Rejected %d invalid customer rows -> _rejected/customers", invalid_df.count())

    logger.info("Customers written: %d rows -> %s/customers", valid_df.count(), silver_path)
    return valid_df


def process_accounts(spark: SparkSession, cfg, raw_path: str, silver_path: str,
                      customers_df, batch_id: str):
    logger.info("Processing accounts: %s", raw_path)
    df = spark.read.option("header", True).csv(raw_path)
    df = df.withColumn("balance", df["balance"].cast(DoubleType()))

    df = (
        df.transform(cleaning.standardize_column_names)
          .transform(cleaning.trim_string_columns)
          .transform(lambda d: cleaning.standardize_dates(d, ["opened_date", "updated_at"]))
          .transform(lambda d: cleaning.deduplicate(d, ["account_id"], order_by="updated_at"))
          .transform(lambda d: cleaning.standardize_currency(
              d, "balance", "currency", cfg.get("etl", "invalid_transaction_rules", "allowed_currencies")))
          .transform(lambda d: validation.validate_id_format(d, "account_id", r"^ACC\d{7}$"))
          .transform(lambda d: validation.validate_referential_integrity(
              d, customers_df, "customer_id", "customer_id"))
          .transform(lambda d: business_rules.add_account_tenure_days(d, "opened_date"))
          .transform(lambda d: business_rules.enrich_with_processing_metadata(d, "core_banking_csv", batch_id))
    )

    valid_df = df.filter(
        "account_id_is_valid = true AND customer_id_is_valid = true AND currency_is_valid = true"
    )
    invalid_df = df.exceptAll(valid_df)

    valid_df.write.mode("overwrite").format("parquet").save(f"{silver_path}/accounts")
    if invalid_df.count() > 0:
        invalid_df.write.mode("overwrite").format("parquet").save(f"{silver_path}/_rejected/accounts")
        logger.warning("Rejected %d invalid account rows -> _rejected/accounts", invalid_df.count())

    logger.info("Accounts written: %d rows -> %s/accounts", valid_df.count(), silver_path)
    return valid_df


def process_payments(spark: SparkSession, cfg, raw_path: str, silver_path: str,
                      accounts_df, batch_id: str):
    logger.info("Processing payments: %s", raw_path)
    df = spark.read.json(raw_path)   # newline-delimited JSON
    rules = cfg.get("etl", "invalid_transaction_rules")

    df = (
        df.transform(cleaning.standardize_column_names)
          .transform(lambda d: cleaning.deduplicate(d, ["payment_id"], order_by="updated_at"))
          .transform(lambda d: cleaning.standardize_currency(
              d, "amount", "currency", rules["allowed_currencies"]))
          .transform(lambda d: validation.validate_amount_range(
              d, "amount", rules["min_amount"], rules["max_amount"]))
          .transform(lambda d: validation.validate_referential_integrity(
              d, accounts_df, "from_account_id", "account_id"))
          .transform(business_rules.add_payment_size_bucket)
          .transform(lambda d: business_rules.enrich_with_processing_metadata(d, "payments_api", batch_id))
    )

    valid_df = business_rules.filter_invalid_transactions(
        df, ["amount_is_valid", "currency_is_valid", "from_account_id_is_valid"]
    )
    invalid_df = df.exceptAll(valid_df)

    valid_df.write.mode("append").format("parquet").partitionBy("status").save(f"{silver_path}/payments")
    if invalid_df.count() > 0:
        invalid_df.write.mode("append").format("parquet").save(f"{silver_path}/_rejected/payments")
        logger.warning("Rejected %d invalid payment rows -> _rejected/payments", invalid_df.count())

    logger.info("Payments written: %d valid rows -> %s/payments", valid_df.count(), silver_path)
    return valid_df


def process_savings_transactions(spark: SparkSession, cfg, raw_path: str, silver_path: str,
                                  accounts_df, batch_id: str):
    logger.info("Processing savings transactions: %s", raw_path)
    df = spark.read.json(raw_path)

    df = (
        df.transform(cleaning.standardize_column_names)
          .transform(lambda d: cleaning.deduplicate(d, ["transaction_id"], order_by="updated_at"))
          .transform(lambda d: cleaning.handle_missing_values(d, {"description": "Not Provided"}))
          .transform(lambda d: validation.validate_referential_integrity(
              d, accounts_df, "account_id", "account_id"))
          .transform(business_rules.add_transaction_direction)
          .transform(lambda d: business_rules.enrich_with_processing_metadata(d, "core_banking_json", batch_id))
    )

    valid_df = df.filter("account_id_is_valid = true")
    invalid_df = df.exceptAll(valid_df)

    valid_df.write.mode("append").format("parquet").partitionBy("transaction_direction").save(
        f"{silver_path}/savings_transactions")
    if invalid_df.count() > 0:
        invalid_df.write.mode("append").format("parquet").save(f"{silver_path}/_rejected/savings_transactions")

    logger.info("Savings txns written: %d valid rows -> %s/savings_transactions", valid_df.count(), silver_path)
    return valid_df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--running-on-databricks", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    batch_id = str(uuid.uuid4())[:8]
    logger.info("Starting bronze_to_silver job. batch_id=%s", batch_id)

    raw_base = cfg.path_for("raw", args.running_on_databricks)
    silver_base = cfg.path_for("silver", args.running_on_databricks)

    spark = get_spark()

    try:
        customers_df = process_customers(spark, cfg, f"{raw_base}/customers.csv", silver_base, batch_id)
        accounts_df = process_accounts(spark, cfg, f"{raw_base}/accounts.csv", silver_base, customers_df, batch_id)
        process_payments(spark, cfg, f"{raw_base}/payments.json", silver_base, accounts_df, batch_id)
        process_savings_transactions(spark, cfg, f"{raw_base}/savings_transactions.json",
                                      silver_base, accounts_df, batch_id)
        # Branches / products / channels are small reference tables - simple pass-through copy
        for ref in ["branches", "products", "transaction_channels"]:
            ref_df = spark.read.option("header", True).csv(f"{raw_base}/{ref}.csv")
            ref_df = ref_df.transform(cleaning.standardize_column_names).transform(cleaning.trim_string_columns)
            ref_df.write.mode("overwrite").format("parquet").save(f"{silver_base}/{ref}")

        logger.info("bronze_to_silver job completed successfully. batch_id=%s", batch_id)
    except PipelineError:
        logger.exception("Pipeline error during bronze_to_silver job")
        raise
    except Exception:
        logger.exception("Unexpected error during bronze_to_silver job")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
