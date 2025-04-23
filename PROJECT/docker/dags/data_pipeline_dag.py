from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

# -----------------------------Create Default Args --------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------------------Create Dag ----------------------------------
dag = DAG(
    "data_pipeline_dag",
    default_args=default_args,
    description="Run Scripts",
    schedule_interval=None,
)

# ------------------------------------Define Function with matching file names -------------------
def extract_kaggle(): subprocess.run(["python", "/app/script/1_extract_kaggle.py"], check=True)
def load_raw_s3(): subprocess.run(["python", "/app/script/2_load_raw_s3.py"], check=True)
def transform_process(): subprocess.run(["python", "/app/script/3_transform_process.py"], check=True)
def load_to_athena(): subprocess.run(["python", "/app/script/4_load_to_athena.py"], check=True)
def validate_logic(): subprocess.run(["python", "/app/script/5_validate_logic.py"], check=True)
def store_validated_s3(): subprocess.run(["python", "/app/script/6_store_validated_s3.py"], check=True)
def visualize_tableau(): subprocess.run(["python", "/app/script/7_visualize_tableau.py"], check=True)


# --------------------------------------Create Tasks ----------------------------------
task1 = PythonOperator(task_id="extract_kaggle", python_callable=extract_kaggle, dag=dag)
task2 = PythonOperator(task_id="load_raw_s3", python_callable=load_raw_s3, dag=dag)
task3 = PythonOperator(task_id="transform_process", python_callable=transform_process, dag=dag)
task4 = PythonOperator(task_id="load_to_athena", python_callable=load_to_athena, dag=dag)
task5 = PythonOperator(task_id="validate_logic", python_callable=validate_logic, dag=dag)
task6 = PythonOperator(task_id="store_validated_s3", python_callable=store_validated_s3, dag=dag)
task7 = PythonOperator(task_id="visualize_tableau", python_callable=visualize_tableau, dag=dag)


# -----------------------------Task EXecution -------------------------------------------------
task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7
