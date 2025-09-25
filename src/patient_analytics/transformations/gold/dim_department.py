import dlt
from pyspark.sql.functions import concat, sha2, concat_ws, substring, conv, col

catalog = dbutils.widgets.get("catalog")

dlt.create_streaming_table(
    name = f"{catalog}.gold.dim_department",
     table_properties={
        "quality": "gold"
    }
)
@dlt.expect_all(
    {
        "department_not_null": "department IS NOT NULL",
        "hospital_id_not_null": "hospital_id IS NOT NULL"
    }
)

@dlt.view(
    name="department_view"
)
def deduplicated_department():
    sk_expr = conv(substring(sha2(concat_ws("||", col("department"), col("hospital_id").cast("string")), 256), 1, 15), 16, 10).cast("long")
    dep = (dlt.read_stream(f"{catalog}.silver.patients")
           .select('department','hospital_id', 'silver_processing_time')
           .drop_duplicates(['department','hospital_id'])
           )

    # Add SK
    return dep.withColumn(
        "department_sk",
        sk_expr)

dlt.create_auto_cdc_flow(
  target = f"{catalog}.gold.dim_department",
  source = "department_view",
  keys = ["department", "hospital_id"],
  sequence_by = "silver_processing_time",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 1,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = "dim_department_cdc",
  once = False
)

