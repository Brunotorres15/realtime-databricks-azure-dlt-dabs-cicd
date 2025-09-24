from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, unix_timestamp,
    round
)

def add_time_columns(df: DataFrame, start_col: str, end_col: str) -> DataFrame:
    """Converte colunas para timestamp e calcula duração em horas."""
    return (
        df.withColumn(f"{start_col}_ts", to_timestamp(col(start_col)))
          .withColumn(f"{end_col}_ts", to_timestamp(col(end_col)))
          .withColumn("silver_processing_time", current_timestamp())
          .withColumn(
              "length_of_stay_hours",
              round(
                  (unix_timestamp(col(f"{end_col}_ts")) - unix_timestamp(col(f"{start_col}_ts"))) / 3600, 
                  2
              )
          )
    )
