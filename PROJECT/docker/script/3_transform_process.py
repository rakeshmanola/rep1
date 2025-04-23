from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import DateType
from urllib.parse import quote
import os
import logging 

logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

#----------------------Create SparkSession -----------------------------------------
spark = SparkSession.builder \
    .appName("S3DataProcessing") \
    .config("spark.driver.extraClassPath", "/app/script/jars/*") \
    .config("spark.executor.extraClassPath", "/app/script/jars/*") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.jars", "/app/script/jars/hadoop-aws-3.3.4.jar,/app/script/jars/aws-java-sdk-bundle-1.12.262.jar") \
    .getOrCreate()

spark.conf.set("spark.hadoop.native.lib", "false")

#---------------Initialize s3 Bucket Name --------------------------------------
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "manola1")


# --------------------s3 Row DataSet Path Name -------------------------------
cardiovascular_data_path = f"s3a://{S3_BUCKET_NAME}/ROW_DATA/Cardiovascular_Data/data_cardiovascular_risk.csv"
daily_food_nutrition_data_path = f"s3a://{S3_BUCKET_NAME}/ROW_DATA/Daily_Food_Nutrition_Dataset/daily_food_nutrition_dataset.csv"
heart_data_path = f"s3a://{S3_BUCKET_NAME}/ROW_DATA/Heart_Data/Dataset Heart Disease.csv"
sleep_data_path = f"s3a://{S3_BUCKET_NAME}/ROW_DATA/Sleep_Data/Sleep_health_and_lifestyle_dataset.csv"
fitness_data_path = f"s3a://{S3_BUCKET_NAME}/ROW_DATA/User_Fitness_Activity_Data/fitness_tracker.csv"


# --------------------s3 Processed DataSet Path Name -------------------------------
processed_cardiovascular_data_path = f"s3a://{S3_BUCKET_NAME}/PROCESSED_DATA/Cardiovascular_Data"
processed_daily_food_nutrition_data_path = f"s3a://{S3_BUCKET_NAME}/PROCESSED_DATA/Daily_Food_Nutrition_Dataset"
processed_heart_data_path = f"s3a://{S3_BUCKET_NAME}/PROCESSED_DATA/Heart_Data"
processed_sleep_data_path = f"s3a://{S3_BUCKET_NAME}/PROCESSED_DATA/Sleep_Data"
processed_fitness_data_path = f"s3a://{S3_BUCKET_NAME}/PROCESSED_DATA/User_Fitness_Activity_Data"


# ----------------- Create DataFrame for cardiovascular_data & Tranform Data ---------------------
try:
    print("Processing Cardiovascular Data...")
    cardiovascular_data = spark.read.option("header", "true") \
                                    .option("inferSchema", "true") \
                                    .option("mode", "DROPMALFORMED") \
                                    .csv(cardiovascular_data_path)
    cardiovascular_data = cardiovascular_data.toDF(*[col.replace(" ", "_") for col in cardiovascular_data.columns])
    cardiovascular_data = cardiovascular_data.dropna().dropDuplicates()
    cardiovascular_data.write.mode("overwrite").option("header", True).parquet(processed_cardiovascular_data_path)
    print("Successfully processed Cardiovascular Data")
except Exception as e:
    logging.error(f"Error in Cardiovascular Data processing: {e}")


# ----------------- Create DataFrame for daily_food_nutrition_data & Tranform Data ---------------------

try:
    print("Processing Daily Food Nutrition Data...")
    daily_food_nutrition_data = spark.read.option("header", "true") \
                                          .option("inferSchema", "true") \
                                          .option("mode", "DROPMALFORMED") \
                                          .csv(daily_food_nutrition_data_path)
    daily_food_nutrition_data = daily_food_nutrition_data.toDF(*[col.split(" ")[0] for col in daily_food_nutrition_data.columns])
    daily_food_nutrition_data = daily_food_nutrition_data.toDF(*[col.split(" ")[0] for col in daily_food_nutrition_data.columns])
    daily_food_nutrition_data = daily_food_nutrition_data.withColumn("Date", to_date(daily_food_nutrition_data["Date"], "yyyy-MM-dd"))
    daily_food_nutrition_data = daily_food_nutrition_data.dropna().dropDuplicates()
    daily_food_nutrition_data.write.mode("overwrite").option("header", True).parquet(processed_daily_food_nutrition_data_path)
    print("Successfully processed Daily Food Nutrition Data")
except Exception as e:
    logging.error(f"Error in Daily Food Nutrition Data processing: {e}")


# ----------------- Create DataFrame for Heart data & Tranform Data ---------------------
try:
    print("Processing Heart Data...")
    heart_data = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .option("mode", "DROPMALFORMED") \
                           .csv(heart_data_path)
    heart_data = heart_data.toDF(*[col.replace(" ", "_") for col in heart_data.columns])
    heart_data = heart_data.dropna().dropDuplicates()
    if "_c0" in heart_data.columns:
        heart_data = heart_data.drop("_c0")
    heart_data.write.mode("overwrite").option("header", True).parquet(processed_heart_data_path)
    print("Successfully processed Heart Data")
except Exception as e:
    logging.error(f"Error in Heart Data processing: {e}")


# ----------------- Create DataFrame for Sleep Data & Tranform Data ---------------------
try:
    print("Processing Sleep Data...")
    sleep_data = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .option("mode", "DROPMALFORMED") \
                           .csv(sleep_data_path)
    sleep_data = sleep_data.toDF(*[col.replace(" ", "_") for col in sleep_data.columns])
    sleep_data = sleep_data.dropna().dropDuplicates()
    sleep_data.write.mode("overwrite").option("header", True).parquet(processed_sleep_data_path)
    print("Successfully processed Sleep Data")
except Exception as e:
    logging.error(f"Error in Sleep Data processing: {e}")

# ----------------- Create DataFrame for Fitness data & Tranform Data ---------------------
try:
    print("Processing Fitness Data...")
    fitness_data = spark.read.option("header", "true") \
                             .option("inferSchema", "true") \
                             .option("mode", "DROPMALFORMED") \
                             .csv(fitness_data_path)
    fitness_data = fitness_data.toDF(*[col.replace(" ", "_") for col in fitness_data.columns])
    fitness_data = fitness_data.dropna().dropDuplicates()
    fitness_data.write.mode("overwrite").option("header", True).parquet(processed_fitness_data_path)
    print("Successfully processed Fitness Data")
except Exception as e:
    logging.error(f"Error in Fitness Data processing: {e}")

spark.stop()
