import dlt
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


landing_path = dbutils.secrets.get(scope = "healthcare-analytics", key = "adls-landing-path")


@dlt.table(
    name="dev.bronze.patients",
    table_properties = {
        "quality": "bronze"
    }
)
def patients():
    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(landing_path)
    )
  
    return (
        raw_stream.withColumn(
            "bronze_ingestion_ts", current_timestamp())
        )
        

