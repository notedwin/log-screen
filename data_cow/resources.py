from dagster import (
    IOManager,
    io_manager,
)

from sqlalchemy import create_engine
from sqlalchemy import inspect

import pandas as pd

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Sequence

import requests

class PostgresIOManager(IOManager):
    """IOManager for postgres databases.
    """

    def __init__(self):
        self.db_url = EnvVar("DB_URL")
        self._engine = sqlalchemy.create_engine(self.db_url)
        self._inspector = inspect(self._engine)

    def load_input(self) -> pd.DataFrame:
        """Load the contents of a table as a pandas DataFrame."""
        sql = context.sql
        return pd.read_sql_query(sql, self._engine)

    def handle_output(self, obj):
        if isinstance(obj, pd.DataFrame):
            return obj.to_sql("new", self._engine, if_exists="append", index=False)
        elif obj is None:
            # dbt has already written the data to this table
            pass
        else:
            raise ValueError(f"Unsupported object type {type(obj)} for PGIOManager.")


@io_manager
def postgres_io_manager():
    return PostgresIOManager()