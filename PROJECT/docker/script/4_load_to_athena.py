from pyspark.sql import SparkSession
import boto3
import os
import logging

# ------------------------Create Saprk Session -------------------------------------------
spark = SparkSession.builder.appName("AthenaAutomation").getOrCreate()


# -----------------------Load AWS Credentials -------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

# ----------------------Load S3 Bucket Name --------------------------------------------------
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "manola1")


# ----------------------Initialize S3 Paths ---------------------------------------------------
fitness_data_s3 = f"s3://{S3_BUCKET_NAME}/PROCESSED_DATA/User_Fitness_Activity_Data/"
cardiovascular_data_s3 = f"s3://{S3_BUCKET_NAME}/PROCESSED_DATA/Cardiovascular_Data/"
sleep_data_s3 = f"s3://{S3_BUCKET_NAME}/PROCESSED_DATA/Sleep_Data/"
heart_data_s3 = f"s3://{S3_BUCKET_NAME}/PROCESSED_DATA/Heart_Data/"
food_nutrition_data_s3 = f"s3://{S3_BUCKET_NAME}/PROCESSED_DATA/Daily_Food_Nutrition_Dataset/"


# ----------------------------Initialize Athena Client -------------------------------------------------
client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
)

# ---------------------------- Table Creation Queries ------------------------------------------
table_queries = [
    # -------------------------Fitness Table-----------------------------------------
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS db1.fitness_data (
        User_ID INT,
        Steps INT,
        Heart_Rate INT,
        Calories_Burned INT,
        BMI DOUBLE,
        Workout_Intensity STRING,
        Active_Minutes INT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION '{fitness_data_s3}'
    TBLPROPERTIES ('parquet.compress'='SNAPPY');
    """,
    
    
    # -------------------------------Cardiovascular Table---------------------------------------
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS db1.cardiovascular_data (
        id INT,
        age INT,
        education DOUBLE,
        sex STRING,
        is_smoking STRING,
        cigsPerDay DOUBLE,
        BPMeds DOUBLE,
        prevalentStroke INT,
        prevalentHyp INT,
        diabetes INT,
        totChol DOUBLE,
        sysBP DOUBLE,
        diaBP DOUBLE,
        BMI DOUBLE,
        heartRate DOUBLE,
        glucose DOUBLE,
        TenYearCHD INT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION '{cardiovascular_data_s3}'
    TBLPROPERTIES ('parquet.compress'='SNAPPY');
    """,

    # -------------------------------Sleep Table-------------------------------------------------
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS db1.sleep_data (
        Person_ID INT,
        Gender STRING,
        Age INT,
        Occupation STRING,
        Sleep_Duration DOUBLE,
        Quality_of_Sleep INT,
        Physical_Activity_Level INT,
        Stress_Level INT,
        BMI_Category STRING,
        Blood_Pressure STRING,
        Heart_Rate INT,
        Daily_Steps INT,
        Sleep_Disorder STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION '{sleep_data_s3}'
    TBLPROPERTIES ('parquet.compress'='SNAPPY');
    """,

    # ---------------------------------Heart Table---------------------------------------------
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS db1.heart_data (
        age INT,
        sex INT,
        chest_pain_type INT,
        resting_bps INT,
        cholesterol DOUBLE,
        fasting_blood_sugar INT,
        resting_ecg INT,
        max_heart_rate INT,
        exercise_angina INT,
        oldpeak DOUBLE,
        ST_slope INT,
        target INT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION '{heart_data_s3}'
    TBLPROPERTIES ('parquet.compress'='SNAPPY');
    """,

    # -------------------------------------Daily Food Nutrition Table ---------------------------------
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS db1.daily_food_nutrition_data (
        Date DATE,
        User_ID INT,
        Food_Item STRING,
        Category STRING,
        Calories INT,
        Protein DOUBLE,
        Carbohydrates DOUBLE,
        Fat DOUBLE,
        Fiber DOUBLE,
        Sugars DOUBLE,
        Sodium INT,
        Cholesterol INT,
        Meal_Type STRING,
        Water_Intake INT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION '{food_nutrition_data_s3}'
    TBLPROPERTIES ('parquet.compress'='SNAPPY');
    """
]

# -------------------------------------Execute queries using Boto3---------------------------
try:
    for query in table_queries:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "db1"},
            ResultConfiguration={"OutputLocation": f"s3://{S3_BUCKET_NAME}/athena_results/"}
        )
    print("All Athena Tables created successfully.")

except Exception as e:
    logging.error(f"Error in table creation: {e}")

# ----------------------------------------Refresh all tables in Athena-----------------------------
try:
    refresh_queries = [
        "MSCK REPAIR TABLE db1.fitness_data;",
        "MSCK REPAIR TABLE db1.cardiovascular_data;",
        "MSCK REPAIR TABLE db1.sleep_data;",
        "MSCK REPAIR TABLE db1.heart_data;",
        "MSCK REPAIR TABLE db1.daily_food_nutrition_data;"
    ]

    for query in refresh_queries:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "db1"},
            ResultConfiguration={"OutputLocation": f"s3://{S3_BUCKET_NAME}/athena_results/"}
        )
    print("Athena Tables refreshed successfully.")

except Exception as e:
    logging.error(f"Error in table repair: {e}")

