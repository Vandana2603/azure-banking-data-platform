"""
conftest.py
===========
Shared pytest fixtures - a local, single-JVM SparkSession used across all
unit tests so we don't spin up a new one per test module.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pyspark"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-bank-platform")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
