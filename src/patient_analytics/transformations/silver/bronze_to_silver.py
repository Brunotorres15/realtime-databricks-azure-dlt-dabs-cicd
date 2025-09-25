import dlt
from pyspark.sql.functions import col, when, current_timestamp, unix_timestamp, round, hour, dayofweek, month, to_timestamp
from utilities import add_time_columns, enrich_with_temporal_attrs

catalog = dbutils.widgets.get("catalog")
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
    df = dlt.read_stream(f"{catalog}.{bronze_schema}.patients")
    df = add_time_columns.add_time_columns(df, "admission_time", "discharge_time")
    df = enrich_with_temporal_attrs.enrich_with_temporal_attrs(df, "admission_time_ts")
    return df


