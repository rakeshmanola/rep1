import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3

# ----------------------- Create SparkSession ----------------------------------------
spark = SparkSession.builder.appName("upload_to_s3").config("spark.executor.memory", "2g").getOrCreate()

# -------------------------AWS S3 Configuration---------------------------------------
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "manola1")
LOCAL_DATA_PATH = "/app/data/ROW_DATA"

# ---------------------------------------Initialize S3 Client --------------------------------------- 
try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
except Exception as e:
    raise RuntimeError(f"Failed to initialize S3 client: {e}")

# ---------------------------------------Upload Data to s3 -------------------------------------------
def upload_files_to_s3():
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        raise ValueError("AWS credentials are missing! Set AWS_ACCESS_KEY and AWS_SECRET_KEY as environment variables.")

    if not os.path.exists(LOCAL_DATA_PATH):
        raise FileNotFoundError(f"Local data path '{LOCAL_DATA_PATH}' does not exist!")

    folders = [folder for folder in os.listdir(LOCAL_DATA_PATH) if os.path.isdir(os.path.join(LOCAL_DATA_PATH, folder))]

    for folder_name in folders:
        folder_path = os.path.join(LOCAL_DATA_PATH, folder_name)

        # ------------------ Load dataset as a Py-Spark DataFrame ----------------------------------------------
        try:
            df = spark.read.option("header", "true").csv(folder_path)
        except Exception as e:
            print(f"Skipping {folder_name}: Unable to load as DataFrame. Error: {e}")
            continue

        # --------------------Create Partition & upload to S3 ------------------------------------------------
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            s3_key = f"ROW_DATA/{folder_name}/{file_name}"

            print(f"Uploading {file_name} to S3 bucket '{S3_BUCKET_NAME}'...")
            try:
                s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_key)
                print(f"Successfully uploaded: {file_name} -> s3://{S3_BUCKET_NAME}/{s3_key}")
            except Exception as e:
                print(f"Failed to upload {file_name}: {e}")

    print("Files Loaded to s3 - SUCESSFULLY")

if __name__ == "__main__":
    upload_files_to_s3()
