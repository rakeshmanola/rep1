from pyspark.sql import SparkSession
from pyathena import connect
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --------------------------- Create Spark Session -----------------------------------
spark = SparkSession.builder.appName("Health Analysis").config("spark.sql.catalogImplementation", "hive").getOrCreate()

DATABASE = "db1"
S3_OUTPUT_PATH = "s3://manola1/athena_results/"

# -------------------------- Create Athena Connection --------------------------------
conn = connect(s3_staging_dir=S3_OUTPUT_PATH, region_name=os.getenv("AWS_DEFAULT_REGION"))
cursor = conn.cursor()

# -----------------------------Function to execute Athena queries and return PySpark DataFrame ---------------------
def query_athena(sql_query, schema):
    cursor.execute(sql_query)
    results = cursor.fetchall()
    return spark.createDataFrame(results, schema=schema) if results else spark.createDataFrame([], schema=schema)

# --------------------------- Define Table Schemas -----------------------------------
schema_mappings = {
    "cardiovascular_data": StructType([
        StructField("id", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("education", DoubleType(), True),
        StructField("sex", StringType(), True),
        StructField("is_smoking", StringType(), True),
        StructField("cigsperday", DoubleType(), True),
        StructField("bpmeds", DoubleType(), True),
        StructField("prevalentstroke", IntegerType(), True),
        StructField("prevalenthyp", IntegerType(), True),
        StructField("diabetes", IntegerType(), True),
        StructField("totchol", DoubleType(), True),
        StructField("sysbp", DoubleType(), True),
        StructField("diabp", DoubleType(), True),
        StructField("bmi", DoubleType(), True),
        StructField("heartrate", DoubleType(), True),
        StructField("glucose", DoubleType(), True),
        StructField("tenyearchd", IntegerType(), True)
    ]),
    "daily_food_nutrition_data": StructType([
        StructField("date", DateType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("food_item", StringType(), True),
        StructField("category", StringType(), True),
        StructField("calories", IntegerType(), True),
        StructField("protein", DoubleType(), True),
        StructField("carbohydrates", DoubleType(), True),
        StructField("fat", DoubleType(), True),
        StructField("fiber", DoubleType(), True),
        StructField("sugars", DoubleType(), True),
        StructField("sodium", IntegerType(), True),
        StructField("cholesterol", IntegerType(), True),
        StructField("meal_type", StringType(), True),
        StructField("water_intake", IntegerType(), True)
    ]),
    "fitness_data": StructType([
        StructField("user_id", IntegerType(), True),
        StructField("steps", IntegerType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("calories_burned", IntegerType(), True),
        StructField("bmi", DoubleType(), True),
        StructField("workout_intensity", StringType(), True),
        StructField("active_minutes", IntegerType(), True),
    ]),
    "heart_data": StructType([
        StructField("age", IntegerType(), True),
        StructField("sex", IntegerType(), True),
        StructField("chest_pain_type", IntegerType(), True),
        StructField("resting_bps", IntegerType(), True),
        StructField("cholesterol", DoubleType(), True),
        StructField("fasting_blood_sugar", IntegerType(), True),
        StructField("resting_ecg", IntegerType(), True),
        StructField("max_heart_rate", IntegerType(), True),
        StructField("exercise_angina", IntegerType(), True),
        StructField("oldpeak", DoubleType(), True),
        StructField("st_slope", IntegerType(), True),
        StructField("target", IntegerType(), True),
    ]),
    "sleep_data": StructType([
        StructField("person_id", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("occupation", StringType(), True),
        StructField("sleep_duration", DoubleType(), True),
        StructField("quality_of_sleep", IntegerType(), True),
        StructField("physical_activity_level", IntegerType(), True),
        StructField("stress_level", IntegerType(), True),
        StructField("bmi_category", StringType(), True),
        StructField("blood_pressure", StringType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("daily_steps", IntegerType(), True),
        StructField("sleep_disorder", StringType(), True),
    ])
}

# -------------------------- Load Data from Athena -----------------------------------
dfs = {table: query_athena(f"SELECT * FROM {DATABASE}.{table}", schema) for table, schema in schema_mappings.items()}

# ----------------------------- Merge Tables ----------------------------------------

health_analysis_df = dfs["cardiovascular_data"].alias("cardio") \
    .join(dfs["daily_food_nutrition_data"].alias("food"), col("cardio.id") == col("food.user_id"), "left") \
    .join(dfs["fitness_data"].alias("fitness"), col("cardio.id") == col("fitness.user_id"), "left") \
    .join(dfs["heart_data"].alias("heart"), col("cardio.age") == col("heart.age"), "left") \
    .join(dfs["sleep_data"].alias("sleep"), col("cardio.id") == col("sleep.person_id"), "left") \
    .select(
        col("cardio.id").alias("user_id"),
        col("cardio.age").alias("cardio_age"),
        col("heart.age").alias("heart_age"),
        col("sleep.age").alias("sleep_age"),
        col("cardio.sex"),
        col("cardio.bmi"),
        col("cardio.sysbp"),
        col("cardio.diabp"),
        col("cardio.glucose"),
        col("cardio.tenyearchd"),
        col("food.food_item"),
        col("food.calories"),
        col("food.protein"),
        col("food.fat"),
        col("fitness.steps"),
        col("fitness.heart_rate"),
        col("fitness.active_minutes"),
        col("heart.cholesterol"),
        col("heart.max_heart_rate"),
        col("heart.exercise_angina"),
        col("sleep.sleep_duration"),
        col("sleep.stress_level"),
        col("sleep.quality_of_sleep")
    )


# ------------------- Remove NULLs and Optimize Data --------------------------------
health_analysis_df = health_analysis_df.dropna().dropDuplicates()

# ------------------- Create Athena Table for Health Analysis ----------------------
create_health_analysis_query = f"""
CREATE TABLE {DATABASE}.health_analysis AS
SELECT 
    cardio.id AS user_id,
    cardio.age,
    cardio.sex,
    cardio.bmi,
    cardio.sysbp,
    cardio.diabp,
    cardio.glucose,
    cardio.tenyearchd,
    food.food_item,
    food.calories,
    food.protein,
    food.fat,
    fitness.steps,
    fitness.heart_rate,
    fitness.active_minutes,
    heart.cholesterol,
    heart.max_heart_rate,
    heart.exercise_angina,
    sleep.sleep_duration,
    sleep.stress_level,
    sleep.quality_of_sleep
FROM {DATABASE}.cardiovascular_data AS cardio
LEFT JOIN {DATABASE}.daily_food_nutrition_data AS food ON cardio.id = food.user_id
LEFT JOIN {DATABASE}.fitness_data AS fitness ON cardio.id = fitness.user_id
LEFT JOIN {DATABASE}.heart_data AS heart ON cardio.age = heart.age
LEFT JOIN {DATABASE}.sleep_data AS sleep ON cardio.id = sleep.person_id;
"""

cursor.execute(create_health_analysis_query)
print("Athena table 'health_analysis' created successfully.")

print("Health data processing completed successfully.")
