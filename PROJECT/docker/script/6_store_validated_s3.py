import boto3
import os
import pandas as pd
import time

# ------------------------ AWS Credentials & Config ------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "ap-south-1"

# ------------------------ Athena & S3 Configuration -----------------------
DATABASE_NAME = "db1"
TABLE_NAME = "health_analysis"
QUERY_STRING = f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}"
S3_OUTPUT_PATH = "s3://manola1/athena_results/"
LOCAL_PARQUET_PATH = "health_analysis.parquet"

# ------------------------ Initialize Athena Client ------------------------
athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# ------------------------ Execute Athena Query ------------------------
try:
    query_execution = athena_client.start_query_execution(
        QueryString=QUERY_STRING,
        QueryExecutionContext={"Database": DATABASE_NAME},
        ResultConfiguration={"OutputLocation": S3_OUTPUT_PATH}
    )
    query_execution_id = query_execution["QueryExecutionId"]
    print(f"Query Execution ID: {query_execution_id}")
except Exception as e:
    print(f"Error executing Athena query: {e}")
    exit(1)

# ------------------------ Wait for Query Completion ------------------------
status = "RUNNING"
while status in ["RUNNING", "QUEUED"]:
    time.sleep(2)
    response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    status = response["QueryExecution"]["Status"]["State"]
    print(f"Current Status: {status}")

# ------------------------ Fetch & Process Query Results ------------------------
if status == "SUCCEEDED":
    try:
        result_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        rows = result_response["ResultSet"]["Rows"]

        # -----------------------------Convert results to Pandas DataFrame ------------------------
        data = [[col.get("VarCharValue", "") for col in row["Data"]] for row in rows if "Data" in row]

        # --------------------------Create DataFrame and set column names--------------------------- 
        column_info = result_response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        column_names = [col["Name"] for col in column_info]
        df = pd.DataFrame(data[1:], columns=column_names)

        # --------------------------Save as Parquet ------------------------------------------
        df.to_parquet(LOCAL_PARQUET_PATH, index=False)
        print(f"Results saved to `{LOCAL_PARQUET_PATH}` in Parquet format.")

        # ------------------------ Upload Parquet File to S3 ------------------------
        s3_client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        s3_client.upload_file(LOCAL_PARQUET_PATH, "manola1", "health_analysis.parquet")
        print("Results uploaded to S3 successfully!")

    except Exception as e:
        print(f"Error processing Athena results: {e}")
else:
    print(f"Query failed with status: {status}")
