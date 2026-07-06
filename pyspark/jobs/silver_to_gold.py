"""
silver_to_gold.py
==================
Builds the star-schema gold layer (dimension + fact tables with surrogate
keys) from the cleaned silver datasets. This is the layer that DBT sources
from / that gets loaded into Azure SQL Database (see sql/warehouse_load.sql).

Run:
    spark-submit pyspark/jobs/silver_to_gold.py --config config/config.yaml
"""

import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, functions as F, Window

from utils.config_loader import load_config
from utils.logger import get_logger

logger = get_logger(__name__)


def get_spark(app_name: str = "BankSilverToGold") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def add_surrogate_key(df, key_name: str):
    """Generate a stable-looking incremental surrogate key.
    (In production this would use IDENTITY columns in Azure SQL or a
    Delta Lake merge-generated key; monotonically_increasing_id is fine
    for a portfolio-scale batch job.)
    """
    return df.withColumn(key_name, F.monotonically_increasing_id() + 1)


def build_dim_customer(spark, silver_path, gold_path):
    df = spark.read.parquet(f"{silver_path}/customers")
    dim = df.select(
        "customer_id", "full_name", "email", "phone", "date_of_birth",
        "gender", "city", "state", "segment", "kyc_status", "age", "age_group",
    ).dropDuplicates(["customer_id"])
    dim = add_surrogate_key(dim, "customer_sk")
    dim.write.mode("overwrite").format("parquet").save(f"{gold_path}/dim_customer")
    logger.info("dim_customer: %d rows", dim.count())
    return dim


def build_dim_account(spark, silver_path, gold_path):
    df = spark.read.parquet(f"{silver_path}/accounts")
    dim = df.select(
        "account_id", "customer_id", "branch_id", "product_id",
        "account_status", "opened_date", "currency", "account_tenure_days",
    ).dropDuplicates(["account_id"])
    dim = add_surrogate_key(dim, "account_sk")
    dim.write.mode("overwrite").format("parquet").save(f"{gold_path}/dim_account")
    logger.info("dim_account: %d rows", dim.count())
    return dim


def build_dim_branch(spark, silver_path, gold_path):
    df = spark.read.parquet(f"{silver_path}/branches")
    dim = add_surrogate_key(df, "branch_sk")
    dim.write.mode("overwrite").format("parquet").save(f"{gold_path}/dim_branch")
    logger.info("dim_branch: %d rows", dim.count())
    return dim


def build_dim_product(spark, silver_path, gold_path):
    df = spark.read.parquet(f"{silver_path}/products")
    dim = add_surrogate_key(df, "product_sk")
    dim.write.mode("overwrite").format("parquet").save(f"{gold_path}/dim_product")
    logger.info("dim_product: %d rows", dim.count())
    return dim


def build_dim_date(spark, gold_path, start_date="2020-01-01", end_date="2027-12-31"):
    """Generate a standard date dimension spanning the full data range."""
    df = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date")
    dim = (
        df.withColumn("date_sk", F.date_format("full_date", "yyyyMMdd").cast("int"))
          .withColumn("year", F.year("full_date"))
          .withColumn("quarter", F.quarter("full_date"))
          .withColumn("month", F.month("full_date"))
          .withColumn("month_name", F.date_format("full_date", "MMMM"))
          .withColumn("day", F.dayofmonth("full_date"))
          .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
          .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
    )
    dim.write.mode("overwrite").format("parquet").save(f"{gold_path}/dim_date")
    logger.info("dim_date: %d rows", dim.count())
    return dim


def build_fact_payments(spark, silver_path, gold_path, dim_account, dim_date):
    df = spark.read.parquet(f"{silver_path}/payments")
    df = df.withColumn("date_key", F.date_format("payment_timestamp", "yyyyMMdd").cast("int"))

    fact = (
        df.join(dim_account.select(F.col("account_id").alias("from_account_id"),
                                    F.col("account_sk").alias("from_account_sk")),
                on="from_account_id", how="left")
          .join(dim_account.select(F.col("account_id").alias("to_account_id"),
                                    F.col("account_sk").alias("to_account_sk")),
                on="to_account_id", how="left")
          .join(dim_date.select(F.col("date_sk"), F.col("date_sk").alias("date_key_join")),
                df["date_key"] == F.col("date_key_join"), how="left")
          .select(
              "payment_id", "from_account_sk", "to_account_sk", "date_sk",
              "amount", "currency", "payment_type", "channel_id", "status",
              "payment_timestamp", "payment_size_bucket",
          )
    )
    fact.write.mode("overwrite").format("parquet").partitionBy("date_sk").save(f"{gold_path}/fact_payments")
    logger.info("fact_payments: %d rows", fact.count())
    return fact


def build_fact_savings_transactions(spark, silver_path, gold_path, dim_account, dim_date):
    df = spark.read.parquet(f"{silver_path}/savings_transactions")
    df = df.withColumn("date_key", F.date_format("transaction_timestamp", "yyyyMMdd").cast("int"))

    fact = (
        df.join(dim_account, on="account_id", how="left")
          .join(dim_date.select(F.col("date_sk"), F.col("date_sk").alias("date_key_join")),
                df["date_key"] == F.col("date_key_join"), how="left")
          .select(
              "transaction_id", "account_sk", "date_sk", "transaction_type",
              "transaction_direction", "amount", "channel_id", "transaction_timestamp",
          )
    )
    fact.write.mode("overwrite").format("parquet").partitionBy("date_sk").save(
        f"{gold_path}/fact_savings_transactions")
    logger.info("fact_savings_transactions: %d rows", fact.count())
    return fact


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--running-on-databricks", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    silver_path = cfg.path_for("silver", args.running_on_databricks)
    gold_path = cfg.path_for("gold", args.running_on_databricks)

    spark = get_spark()
    logger.info("Starting silver_to_gold job")

    try:
        dim_customer = build_dim_customer(spark, silver_path, gold_path)
        dim_account = build_dim_account(spark, silver_path, gold_path)
        build_dim_branch(spark, silver_path, gold_path)
        build_dim_product(spark, silver_path, gold_path)
        dim_date = build_dim_date(spark, gold_path)

        build_fact_payments(spark, silver_path, gold_path, dim_account, dim_date)
        build_fact_savings_transactions(spark, silver_path, gold_path, dim_account, dim_date)

        logger.info("silver_to_gold job completed successfully")
    except Exception:
        logger.exception("silver_to_gold job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
