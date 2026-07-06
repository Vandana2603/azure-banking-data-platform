"""
generate_data.py
=================
Generates a realistic synthetic banking dataset for the Azure Banking Data
Platform portfolio project.

Produces (under data/raw/):
    customers.csv
    accounts.csv
    branches.csv
    products.csv
    transaction_channels.csv
    payments.json
    savings_transactions.json

Run:
    python data/generate_data.py --config config/config.yaml

The data intentionally contains realistic messiness (nulls, duplicate rows,
inconsistent date formats, mixed-case text, a few invalid amounts/currencies)
so the PySpark cleaning/validation modules have real work to do.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

import yaml
from faker import Faker


# --------------------------------------------------------------------------- #
# Setup
# --------------------------------------------------------------------------- #

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def daterange_random(start: datetime, end: datetime) -> datetime:
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def messy_date(dt: datetime) -> str:
    """Return the date in one of several inconsistent formats on purpose,
    to simulate real-world source system inconsistency that the ETL must
    standardize."""
    fmt_choices = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m-%d-%Y",
        "%Y/%m/%d %H:%M:%S",
        "%d-%b-%Y",
    ]
    return dt.strftime(random.choice(fmt_choices))


def maybe_null(value, null_rate: float = 0.02):
    return None if random.random() < null_rate else value


# --------------------------------------------------------------------------- #
# Generators
# --------------------------------------------------------------------------- #

def generate_branches(fake: Faker, n: int) -> List[Dict[str, Any]]:
    branches = []
    for i in range(1, n + 1):
        branches.append({
            "branch_id": f"BR{i:04d}",
            "branch_name": f"{fake.city()} Branch",
            "region": random.choice(["North", "South", "East", "West", "Central"]),
            "city": fake.city(),
            "state": fake.state(),
            "opened_date": fake.date_between(start_date="-20y", end_date="-1y").isoformat(),
            "manager_name": fake.name(),
        })
    return branches


def generate_products(n: int) -> List[Dict[str, Any]]:
    product_catalog = [
        ("Savings Account", "Savings"),
        ("Current Account", "Current"),
        ("Fixed Deposit", "Deposit"),
        ("Recurring Deposit", "Deposit"),
        ("Salary Account", "Savings"),
        ("Student Account", "Savings"),
        ("NRI Savings Account", "Savings"),
        ("Business Current Account", "Current"),
        ("Senior Citizen Savings", "Savings"),
        ("Zero Balance Account", "Savings"),
        ("Premium Current Account", "Current"),
        ("Tax Saver Fixed Deposit", "Deposit"),
    ]
    products = []
    for i, (name, category) in enumerate(product_catalog[:n], start=1):
        products.append({
            "product_id": f"PRD{i:03d}",
            "product_name": name,
            "product_category": category,
            "interest_rate": round(random.uniform(2.5, 7.5), 2),
            "min_balance": random.choice([0, 500, 1000, 2500, 5000]),
        })
    return products


def generate_channels() -> List[Dict[str, Any]]:
    channels = ["Branch", "ATM", "Mobile App", "Internet Banking", "UPI", "POS", "IVR"]
    return [{"channel_id": f"CH{i+1:02d}", "channel_name": c} for i, c in enumerate(channels)]


def generate_customers(fake: Faker, n: int) -> List[Dict[str, Any]]:
    customers = []
    for i in range(1, n + 1):
        first, last = fake.first_name(), fake.last_name()
        name_variant = random.choice([
            f"{first} {last}",
            f"{first.upper()} {last.upper()}",
            f"{first.lower()} {last.lower()}",
        ])
        customers.append({
            "customer_id": f"CUST{i:06d}",
            "full_name": name_variant,
            "email": maybe_null(fake.email(), 0.03),
            "phone": maybe_null(fake.msisdn()[:10], 0.02),
            "date_of_birth": messy_date(fake.date_of_birth(minimum_age=18, maximum_age=85)
                                         if isinstance(fake.date_of_birth(), datetime)
                                         else datetime.combine(fake.date_of_birth(minimum_age=18, maximum_age=85), datetime.min.time())),
            "gender": random.choice(["M", "F", "Other", None]),
            "city": fake.city(),
            "state": fake.state(),
            "segment": random.choice(["Retail", "Premium", "Corporate", "Student"]),
            "kyc_status": random.choice(["Verified", "Pending", "Verified", "Verified"]),
            "created_at": messy_date(fake.date_time_between(start_date="-10y", end_date="-1y")),
            "updated_at": messy_date(fake.date_time_between(start_date="-1y", end_date="now")),
        })
    # Inject a handful of exact duplicate rows to simulate source system dupes
    dupes = random.sample(customers, k=max(1, n // 500))
    customers.extend(dupes)
    return customers


def generate_accounts(fake: Faker, customers: List[dict], branches: List[dict],
                       products: List[dict], n: int) -> List[Dict[str, Any]]:
    accounts = []
    customer_ids = [c["customer_id"] for c in customers]
    branch_ids = [b["branch_id"] for b in branches]
    product_ids = [p["product_id"] for p in products]

    for i in range(1, n + 1):
        opened = fake.date_time_between(start_date="-8y", end_date="now")
        accounts.append({
            "account_id": f"ACC{i:07d}",
            "customer_id": random.choice(customer_ids),
            "branch_id": random.choice(branch_ids),
            "product_id": random.choice(product_ids),
            "account_status": random.choice(["Active", "Active", "Active", "Dormant", "Closed"]),
            "opened_date": messy_date(opened),
            "balance": round(random.uniform(-500, 500000), 2),  # negative = data issue on purpose
            "currency": random.choice(["INR", "USD", "EUR", "INR", "INR", "XYZ"]),  # XYZ = invalid on purpose
            "updated_at": messy_date(fake.date_time_between(start_date=opened, end_date="now")),
        })

    # A few accounts referencing a non-existent customer_id (referential integrity issue)
    for _ in range(max(1, n // 1000)):
        bad = random.choice(accounts).copy()
        bad["account_id"] = f"ACC{random.randint(9000000, 9999999)}"
        bad["customer_id"] = "CUST999999"  # does not exist
        accounts.append(bad)

    return accounts


def generate_payments(fake: Faker, accounts: List[dict], channels: List[dict],
                       n: int, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    account_ids = [a["account_id"] for a in accounts]
    channel_ids = [c["channel_id"] for c in channels]
    payments = []
    for i in range(1, n + 1):
        ts = daterange_random(start, end)
        amount = round(random.uniform(10, 250000), 2)
        # inject a few invalid / outlier amounts on purpose
        if random.random() < 0.005:
            amount = round(random.uniform(-5000, -1), 2)   # negative amount -> invalid
        if random.random() < 0.003:
            amount = 9999999.99                             # unrealistic outlier

        payments.append({
            "payment_id": str(uuid.uuid4()),
            "from_account_id": random.choice(account_ids),
            "to_account_id": random.choice(account_ids),
            "amount": amount,
            "currency": random.choice(["INR", "USD", "EUR", "GBP", "INR", "INR"]),
            "payment_type": random.choice(["NEFT", "RTGS", "IMPS", "UPI", "Wire Transfer", "Card Payment"]),
            "channel_id": random.choice(channel_ids),
            "status": random.choice(["SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"]),
            "payment_timestamp": ts.isoformat(),
            "updated_at": (ts + timedelta(minutes=random.randint(0, 120))).isoformat(),
        })
    return payments


def generate_savings_transactions(fake: Faker, accounts: List[dict], channels: List[dict],
                                   n: int, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    account_ids = [a["account_id"] for a in accounts]
    channel_ids = [c["channel_id"] for c in channels]
    txns = []
    for i in range(1, n + 1):
        ts = daterange_random(start, end)
        txn_type = random.choice(["DEPOSIT", "WITHDRAWAL", "INTEREST_CREDIT", "FEE_DEBIT"])
        amount = round(random.uniform(5, 100000), 2)
        txns.append({
            "transaction_id": str(uuid.uuid4()),
            "account_id": random.choice(account_ids),
            "transaction_type": txn_type,
            "amount": amount,
            "channel_id": random.choice(channel_ids),
            "transaction_timestamp": ts.isoformat(),
            "description": maybe_null(f"{txn_type.title()} via {fake.word()}", 0.05),
            "updated_at": (ts + timedelta(minutes=random.randint(0, 60))).isoformat(),
        })
    return txns


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #

def write_csv(records: List[dict], path: Path):
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        return
    fieldnames = list(records[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"  wrote {len(records):>7,} rows -> {path}")


def write_json(records: List[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")   # newline-delimited JSON (Spark-friendly)
    print(f"  wrote {len(records):>7,} rows -> {path}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic banking data")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--out", default="data/raw")
    args = parser.parse_args()

    cfg = load_config(args.config)["data_generation"]
    random.seed(cfg["seed"])
    fake = Faker()
    Faker.seed(cfg["seed"])

    start = datetime.fromisoformat(cfg["start_date"])
    end = datetime.fromisoformat(cfg["end_date"])
    out_dir = Path(args.out)

    print("Generating dimension data...")
    branches = generate_branches(fake, cfg["num_branches"])
    products = generate_products(cfg["num_products"])
    channels = generate_channels()
    customers = generate_customers(fake, cfg["num_customers"])
    accounts = generate_accounts(fake, customers, branches, products, cfg["num_accounts"])

    print("Generating fact data...")
    payments = generate_payments(fake, accounts, channels, cfg["num_payments"], start, end)
    savings_txns = generate_savings_transactions(fake, accounts, channels,
                                                  cfg["num_savings_transactions"], start, end)

    print("\nWriting output files...")
    write_csv(branches, out_dir / "branches.csv")
    write_csv(products, out_dir / "products.csv")
    write_csv(channels, out_dir / "transaction_channels.csv")
    write_csv(customers, out_dir / "customers.csv")
    write_csv(accounts, out_dir / "accounts.csv")
    write_json(payments, out_dir / "payments.json")
    write_json(savings_txns, out_dir / "savings_transactions.json")

    total = len(branches) + len(products) + len(channels) + len(customers) + \
        len(accounts) + len(payments) + len(savings_txns)
    print(f"\nDone. Total records generated: {total:,}")


if __name__ == "__main__":
    main()
