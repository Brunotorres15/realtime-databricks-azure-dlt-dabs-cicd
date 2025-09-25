"""This file configures pytest."""

import os, sys
import pytest

sys.path.append(os.getcwd())

@pytest.fixture()
def spark():
    """Provide a SparkSession fixture for tests."""
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        except ImportError:
            raise ImportError("Neither databricks-connect nor pyspark is installed.")
    return spark



