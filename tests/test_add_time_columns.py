import os, sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sys.path.append(os.getcwd())

from src.patient_analytics.utilities.add_time_columns import add_time_columns

# ---------- TESTS ----------
def test_add_time_columns_creates_new_columns(spark):
    data = [("2025-01-01 12:00:00", "2025-01-01 15:00:00")]
    df = spark.createDataFrame(data, ["admission_time", "discharge_time"])

    result = add_time_columns(df, "admission_time", "discharge_time")

    # Verifica se as novas colunas foram criadas
    expected_cols = {
        "admission_time", "discharge_time",
        "admission_time_ts", "discharge_time_ts",
        "silver_processing_time", "length_of_stay_hours"
    }
    assert expected_cols.issubset(set(result.columns))


def test_add_time_columns_calculates_length_of_stay(spark):
    data = [("2025-01-01 12:00:00", "2025-01-01 15:00:00")]  # diferença = 3 horas
    df = spark.createDataFrame(data, ["admission_time", "discharge_time"])

    result = add_time_columns(df, "admission_time", "discharge_time")
    row = result.collect()[0]

    assert row["length_of_stay_hours"] == 3.0


def test_add_time_columns_handles_multiple_rows(spark):
    data = [
        ("2025-01-01 12:00:00", "2025-01-01 15:00:00"),  # 3 horas
        ("2025-01-02 08:00:00", "2025-01-02 20:00:00"),  # 12 horas
    ]
    df = spark.createDataFrame(data, ["admission_time", "discharge_time"])

    result = add_time_columns(df, "admission_time", "discharge_time").collect()

    assert result[0]["length_of_stay_hours"] == 3.0
    assert result[1]["length_of_stay_hours"] == 12.0