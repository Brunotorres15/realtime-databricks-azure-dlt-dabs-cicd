import dlt
from pyspark.sql.functions import *

@dlt.table(
    name = "dev.gold.fact_admissions",
    table_properties = {
        "quality": "gold"
    }
)
def fact_admissions():

    incoming_data = (
    dlt.readStream('dev.silver.patients').select('patient_id','admission_time', 'discharge_time', 'bed_id', 'department', 'hospital_id', 'length_of_stay_hours', 'admission_hour', 'admission_day_of_week', 'admission_month', 'stay_category', 'silver_processing_time')
)

    department = dlt.read("dev.gold.dim_department")
    patient = dlt.read("dev.gold.dim_patient")

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


