import dlt
from pyspark.sql.functions import col, when, current_timestamp, unix_timestamp, round, hour, dayofweek, month, to_timestamp

catalog = "dev"
bronze_schema = "bronze"
silver_schema = "silver"


@dlt.table(
    name=f"{catalog}.{silver_schema}.patients",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all({
    "valid_age": "age BETWEEN 0 AND 100",
    "valid_pediatric_age": "department != 'Pediatrics' OR (age BETWEEN 0 AND 18)",
    "valid_admission_time": "admission_time IS NOT NULL",
    "valid_discharge_time": "discharge_time > admission_time"
})
def patients():
    return (
        dlt.read_stream(f"{catalog}.{bronze_schema}.patients")
        # Converte as colunas de string para timestamp
        .withColumn("admission_time_ts", to_timestamp("admission_time"))
        .withColumn("discharge_time_ts", to_timestamp("discharge_time"))
        # Coloca timestamp de processamento
        .withColumn("silver_processing_time", current_timestamp())
        # Calcula duração da internação em horas
        .withColumn(
            "length_of_stay_hours",
            round((unix_timestamp("discharge_time_ts") - unix_timestamp("admission_time_ts")) / 3600, 2)
        )
        # Extrai atributos temporais
        .withColumn("admission_hour", hour("admission_time_ts"))
        .withColumn("admission_day_of_week", dayofweek("admission_time_ts"))
        .withColumn("admission_month", month("admission_time_ts"))
        # Categoriza internação
        .withColumn(
            "stay_category",
            when(col("length_of_stay_hours") < 24, "Short Stay")
            .when(col("length_of_stay_hours") < 72, "Medium Stay")
            .otherwise("Long Stay")
        )
    )
