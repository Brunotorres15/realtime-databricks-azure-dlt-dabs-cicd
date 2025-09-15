import dlt
from pyspark.sql.functions import col, count, avg, round

@dlt.table(
    name="dev.gold.cube_admissions_gender_department",
    comment="Cubos de analytics: número de admissões e média de duração por gênero e departamento",
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

# --- Cubo por departamento e hospital ---
@dlt.table(
    name="dev.gold.cube_admissions_department_hospital",
    comment="Cubos de analytics: número de admissões e média de duração por departamento e hospital",
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
