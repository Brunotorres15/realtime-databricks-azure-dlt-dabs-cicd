import dlt
from pyspark.sql.functions import concat, sha2, desc, col, row_number, concat_ws, substring, conv
from pyspark.sql.window import Window

dlt.create_streaming_table(
    name = "dev.gold.dim_patient",
     table_properties={
        "quality": "gold"
    }
)
@dlt.expect_all(
    {
        "patient_id_not_null": "patient_id IS NOT NULL"
    }
)


@dlt.view(
    name="patient_view"
)
def deduplicated_department():

    sk_expr = conv(substring(sha2(concat_ws("||", col("patient_id"), col("age"), col("gender").cast("string")), 256), 1, 15), 16, 10).cast("long")
    return (dlt.read_stream("dev.silver.patients")
            .withColumn("patient_sk", sk_expr)
           .select("patient_sk", "patient_id", "gender", "age", "silver_processing_time")
           )
    

dlt.create_auto_cdc_flow(
  target = "dev.gold.dim_patient",
  source = "patient_view",
  keys = ["patient_id"],
  sequence_by = "silver_processing_time",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)

