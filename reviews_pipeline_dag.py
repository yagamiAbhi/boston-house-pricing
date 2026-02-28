from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "playground-s-11-40f3175a"
REGION = "us-central1"
CLUSTER_NAME = "reviews-cluster"
BUCKET_NAME = "reviews-pipeline-data"
INPUT_FILE = f"gs://{BUCKET_NAME}/raw/reviews.csv"
PYSPARK_FILE = f"gs://{BUCKET_NAME}/code/process_reviews.py"
BQ_OUTPUT = f"{PROJECT_ID}.reviews_data.cleaned_reviews"
TEMP_BUCKET = BUCKET_NAME  # Reusing same bucket for BQ temp files

# Define the PySpark job config
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_FILE,
        "args": [INPUT_FILE, BQ_OUTPUT],
        "properties": {
            "spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0"
        }
    },
}

with models.DAG(
    "reviews_pipeline_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["reviews", "dataproc", "bigquery"],
) as dag:

    submit_pyspark = DataprocSubmitJobOperator(
        task_id="run_reviews_etl",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )
    
    submit_pyspark
