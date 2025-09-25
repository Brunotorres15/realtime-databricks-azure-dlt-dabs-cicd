import dlt
from pyspark.sql.functions import *

catalog = dbutils.widgets.get("catalog")

@dlt.table(
    name = f"{catalog}.gold.fact_admissions",
    table_properties = {
        "quality": "gold"
    }
)
@dlt.expect_all({
    "valid_patient_fk": "patient_sk IS NOT NULL",
    "valid_department_fk": "department_sk IS NOT NULL",
    "non_negative_stay": "length_of_stay_hours >= 0"
    })
def fact_admissions():

    incoming_data = (
    dlt.readStream(f"{catalog}.silver.patients").select('patient_id','admission_time', 'discharge_time', 'bed_id', 'department', 'hospital_id', 'length_of_stay_hours', 'admission_hour', 'admission_day_of_week', 'admission_month', 'stay_category', 'silver_processing_time')
)

    department = dlt.read(f"{catalog}.gold.dim_department")
    patient = dlt.read(f"{catalog}.gold.dim_patient")

    admissions = (
        incoming_data
            .join(department, on=['department', 'hospital_id'], how='left')
            .join(patient, on='patient_id', how='left')
        )
    
    return (
    admissions.select(
        col("patient_sk"),
        col("department_sk"),
        current_timestamp().alias("event_ingestion_time"),
        "patient_id",
        "admission_time",
        "discharge_time",
        "bed_id",
        "department",
        "hospital_id",
        "length_of_stay_hours",
        "admission_hour",
        "admission_day_of_week",
        "admission_month",
        "stay_category"
    )
)


