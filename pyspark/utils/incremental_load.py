"""
incremental_load.py
====================
Watermark-based incremental loading so daily pipeline runs only process
new/changed records instead of reprocessing the full history every day.

Pattern:
    1. Read the last successful watermark value (max updated_at) from a
       small control table/file.
    2. Filter the source DataFrame to rows with updated_at > watermark.
    3. After a successful load, persist the new max(updated_at) as the
       watermark for the next run.
"""

import json
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, functions as F

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.exceptions import IncrementalLoadError


class WatermarkManager:
    """Simple file-based watermark store.

    In production on Azure this would be a Delta table or a row in Azure SQL
    (see sql/ddl.sql -> etl_watermark table) instead of a local JSON file,
    but the interface stays identical - only `_read`/`_write` change.
    """

    def __init__(self, watermark_path: str = "data/gold/_watermarks.json"):
        self.watermark_path = watermark_path

    def _read_all(self) -> dict:
        if not os.path.exists(self.watermark_path):
            return {}
        with open(self.watermark_path, "r") as f:
            return json.load(f)

    def _write_all(self, data: dict) -> None:
        os.makedirs(os.path.dirname(self.watermark_path), exist_ok=True)
        with open(self.watermark_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def get_watermark(self, dataset_name: str) -> Optional[str]:
        return self._read_all().get(dataset_name)

    def set_watermark(self, dataset_name: str, value: str) -> None:
        data = self._read_all()
        data[dataset_name] = value
        self._write_all(data)


def incremental_filter(df: DataFrame, watermark_col: str,
                        watermark_value: Optional[str]) -> DataFrame:
    """Return only rows newer than the given watermark. If watermark_value
    is None (first run / full load), returns the full DataFrame unchanged.
    """
    if watermark_value is None:
        return df
    try:
        return df.filter(F.col(watermark_col) > F.lit(watermark_value))
    except Exception as exc:
        raise IncrementalLoadError(f"Failed to apply incremental filter: {exc}") from exc


def compute_new_watermark(df: DataFrame, watermark_col: str) -> Optional[str]:
    """Compute the max value of the watermark column from a DataFrame,
    to be persisted after a successful load."""
    result = df.agg(F.max(F.col(watermark_col)).alias("max_wm")).collect()
    if not result or result[0]["max_wm"] is None:
        return None
    return str(result[0]["max_wm"])
