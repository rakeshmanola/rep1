import boto3
import tableauserverclient as TSC
import os
import pandas as pd
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Inserter, CreateMode

# ---------------- Configuration Variables -----------------
BUCKET_NAME = "manola1"
S3_FILE_KEY = "health_analysis.parquet"
LOCAL_PARQUET = "/app/health_analysis.parquet"
LOCAL_CSV = "/app/health_analysis.csv"
LOCAL_HYPER = "/app/health_analysis.hyper"

TABLEAU_SERVER = "https://prod-apsoutheast-b.online.tableau.com"
TABLEAU_USER = os.getenv("TABLEAU_USER")
TABLEAU_PASSWORD = os.getenv("TABLEAU_PASSWORD")
TABLEAU_SITE_ID = os.getenv("TABLEAU_SITE_ID")

# ---------------- Step 1: Download data from S3 -----------------
s3_client = boto3.client("s3")
try:
    s3_client.download_file(BUCKET_NAME, S3_FILE_KEY, LOCAL_PARQUET)
    print("Parquet downloaded from S3.")
except Exception as e:
    print(f"Error downloading Parquet from S3: {e}")
    exit(1)


# ---------------- Step 2: Convert Parquet to CSV -----------------
try:
    df = pd.read_parquet(LOCAL_PARQUET)
    df.to_csv(LOCAL_CSV, index=False)
    print("Parquet converted to CSV.")
except Exception as e:
    print(f"Error converting Parquet to CSV: {e}")
    exit(1)


# ---------------- Step 3: Convert CSV to Hyper -----------------
try:
    with HyperProcess(telemetry="DO_NOT_SEND_USAGE_DATA") as hyper:
        with Connection(endpoint=hyper.endpoint, database=LOCAL_HYPER, create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
            table_columns = [TableDefinition.Column(name, SqlType.text()) for name in df.columns]
            table = TableDefinition("Extract", table_columns)
            conn.catalog.create_table(table)
            
            with Inserter(conn, table) as inserter:
                inserter.add_rows(df.itertuples(index=False, name=None))  # Correct method
                inserter.execute()

    print("CSV converted to Hyper format.")
except Exception as e:
    print(f"Error converting CSV to Hyper: {e}")
    exit(1)

# ---------------- Step 4: Upload Hyper file to Tableau Online -----------------
try:
    tableau_auth = TSC.TableauAuth(TABLEAU_USER, TABLEAU_PASSWORD, TABLEAU_SITE_ID)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)
    
    with server.auth.sign_in(tableau_auth):
        projects, _ = server.projects.get()
        project = next((p for p in projects if p.name == "project1"), None)
        print(f"Tableau Project ID: {project.id if project else 'None'}")

        
        if project:
            datasource_item = TSC.DatasourceItem(project.id)
            server.datasources.publish(datasource_item, LOCAL_HYPER, TSC.Server.PublishMode.Overwrite)
            print("Hyper file published to Tableau!")
        else:
            print("Tableau project not found.")

except Exception as e:
    print(f"Error uploading Hyper file to Tableau: {e}")
    exit(1)
