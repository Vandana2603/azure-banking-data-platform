"""
config_loader.py
=================
Loads config/config.yaml and resolves environment-specific paths so the same
PySpark code can run locally, in Docker, or on Databricks against ADLS Gen2
just by changing `environment` / storage settings in the YAML file.
"""

import os
from typing import Any, Dict

import yaml


class Config:
    """Thin wrapper around the parsed YAML config with convenience accessors."""

    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, "r") as f:
            self._raw: Dict[str, Any] = yaml.safe_load(f)

    def get(self, *keys, default=None):
        """Safe nested get, e.g. cfg.get('data_lake', 'container_raw')."""
        node = self._raw
        for k in keys:
            if not isinstance(node, dict) or k not in node:
                return default
            node = node[k]
        return node

    @property
    def raw(self) -> Dict[str, Any]:
        return self._raw

    def path_for(self, layer: str, running_on_databricks: bool = False) -> str:
        """Return the correct path for a given layer (raw/bronze/silver/gold).

        On Databricks -> returns an abfss:// path against ADLS Gen2.
        Locally/Docker -> returns a local filesystem path under data/.
        """
        if running_on_databricks:
            base = self.get("data_lake", "base_path").format(
                container=self.get("data_lake", f"container_{layer}"),
                storage_account=self.get("data_lake", "storage_account"),
            )
            return base
        return self.get("local_paths", layer)

    def sql_connection_string(self) -> str:
        """Build an ODBC connection string for Azure SQL Database.
        Username/password are read from environment variables - never stored
        in the YAML file or source code.
        """
        server = self.get("sql_warehouse", "server")
        database = self.get("sql_warehouse", "database")
        driver = self.get("sql_warehouse", "driver")
        user = os.environ.get("AZURE_SQL_USER", "")
        pwd = os.environ.get("AZURE_SQL_PASSWORD", "")
        return (
            f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
            f"UID={user};PWD={pwd};Encrypt=yes;TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )


def load_config(config_path: str = "config/config.yaml") -> Config:
    return Config(config_path)
