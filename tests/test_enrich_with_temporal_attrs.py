import sys, os
from pyspark.sql.functions import col
from src.patient_analytics.utilities.enrich_with_temporal_attrs import enrich_with_temporal_attrs

sys.path.append(os.getcwd())

# ---------- TESTS ----------
def test_enrich_with_temporal_attrs_creates_new_columns(spark):
    data = [("2025-01-01 12:00:00", 5.0)]  # 5h de duração
    df = spark.createDataFrame(data, ["admission_time_ts", "length_of_stay_hours"])

    result = enrich_with_temporal_attrs(df, "admission_time_ts")

    expected_cols = {
        "admission_time_ts", "length_of_stay_hours",
        "admission_hour", "admission_day_of_week",
        "admission_month", "stay_category"
    }
    assert expected_cols.issubset(set(result.columns))


def test_stay_category_short_medium_long(spark):
    data = [
        ("2025-01-01 10:00:00", 5.0),    # < 24h -> Short Stay
        ("2025-01-02 08:00:00", 48.0),   # < 72h -> Medium Stay
        ("2025-01-03 09:00:00", 100.0),  # >= 72h -> Long Stay
    ]
    df = spark.createDataFrame(data, ["admission_time_ts", "length_of_stay_hours"])

    result = enrich_with_temporal_attrs(df, "admission_time_ts").collect()

    assert result[0]["stay_category"] == "Short Stay"
    assert result[1]["stay_category"] == "Medium Stay"
    assert result[2]["stay_category"] == "Long Stay"


def test_temporal_attributes_extraction(spark):
    data = [("2025-01-05 14:30:00", 10.0)]  # 5 de janeiro 2025 é domingo
    df = spark.createDataFrame(data, ["admission_time_ts", "length_of_stay_hours"])

    result = enrich_with_temporal_attrs(df, "admission_time_ts").collect()[0]

    assert result["admission_hour"] == 14
    assert result["admission_day_of_week"] == 1  # Spark: 1 = Domingo, 7 = Sábado
    assert result["admission_month"] == 1


def test_stay_category_edge_cases(spark):
    data = [
        ("2025-01-01 08:00:00", 24.0),   # exatamente 24h -> Medium Stay
        ("2025-01-02 08:00:00", 72.0),   # exatamente 72h -> Long Stay
    ]
    df = spark.createDataFrame(data, ["admission_time_ts", "length_of_stay_hours"])

    result = enrich_with_temporal_attrs(df, "admission_time_ts").collect()

    assert result[0]["stay_category"] == "Medium Stay"
    assert result[1]["stay_category"] == "Long Stay"