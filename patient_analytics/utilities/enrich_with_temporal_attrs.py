from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, hour, dayofweek, month, when
)

def enrich_with_temporal_attrs(df: DataFrame, start_ts_col: str, duration_col: str = "length_of_stay_hours") -> DataFrame:
    """Adiciona atributos derivados e categoriza a duração."""
    return (
        df.withColumn("admission_hour", hour(col(start_ts_col)))
          .withColumn("admission_day_of_week", dayofweek(col(start_ts_col)))
          .withColumn("admission_month", month(col(start_ts_col)))
          .withColumn(
              "stay_category",
              when(col(duration_col) < 24, "Short Stay")
              .when(col(duration_col) < 72, "Medium Stay")
              .otherwise("Long Stay")
          )
    )