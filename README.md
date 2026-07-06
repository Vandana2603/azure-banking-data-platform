# Azure Banking Data Platform

A production-style Azure Data Engineering project that simulates how a modern bank processes raw transaction data into analytics-ready datasets using Azure Data Factory, Azure Databricks, PySpark, Azure SQL, DBT, and Power BI.

---

## Tech Stack

- Azure Data Factory
- Azure Databricks
- PySpark
- Azure Data Lake Storage Gen2
- Azure SQL Database
- DBT
- Power BI
- Docker
- Python
- SQL

---

## Architecture

Raw Banking Data
↓
Azure Data Lake Storage
↓
Azure Data Factory
↓
Azure Databricks (PySpark)
↓
Silver Layer
↓
Gold Layer (Star Schema)
↓
Azure SQL Database
↓
DBT
↓
Power BI Dashboard

---

## Features

- Synthetic banking data generation
- Bronze → Silver → Gold ETL pipeline
- Star schema data warehouse
- Data quality validation
- Incremental processing
- DBT transformations
- SQL analytics
- Power BI reporting
- Docker support
- Unit tests

---

## Folder Structure

```
data/
pyspark/
sql/
dbt/
dashboards/
docs/
pipelines/
docker/
tests/
```

---


## Project Highlights

✔ Production-style ETL Pipeline

✔ Modular PySpark Architecture

✔ Star Schema Data Warehouse

✔ Data Validation & Governance

✔ Azure Deployment Ready

✔ Dockerized Development Environment

---

## Future Improvements

- Delta Lake
- CI/CD with GitHub Actions
- Unity Catalog
- CDC Pipeline
- Great Expectations

---

⭐ If you found this project useful, consider giving it a star!
