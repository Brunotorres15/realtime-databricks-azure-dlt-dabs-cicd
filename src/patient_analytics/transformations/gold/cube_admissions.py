import dlt
from pyspark.sql.functions import col, count, avg, round

@dlt.table(
    name="dev.gold.cube_admissions_gender_department",
    comment="# Analytics cubes: number of admissions and average length of stay by gender and department",
    table_properties={"quality": "gold"}
)
def cube_admissions_gender_department():
    return (
        dlt.read_stream("dev.silver.patients")
        .groupBy("gender", "department")
        .agg(
            count("patient_id").alias("num_admissions"),
            round(avg("length_of_stay_hours"), 2).alias("avg_length_of_stay_hours")
        )
    )

# --- Cube by department and hospital ---
@dlt.table(
    name="dev.gold.cube_admissions_department_hospital",
    comment="# Analytics cubes: number of admissions and average length of stay by department and hospital",
    table_properties={"quality": "gold"}
)
def cube_admissions_department_hospital():
    return (
        dlt.read_stream("dev.silver.patients")
        .groupBy("department", "hospital_id")
        .agg(
            count("patient_id").alias("num_admissions"),
            round(avg("length_of_stay_hours"), 2).alias("avg_length_of_stay_hours")
        )
    )
