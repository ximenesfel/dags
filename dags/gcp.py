import os

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.contrib.operators.gcs_list_operator import GCSCreateBucketOperator
from airflow.utils.dates import days_ago
from airflow import DAG

from datetime import timedelta

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "tele-covid19")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "covid-mlengine")

PATH_TO_REMOTE_FILE = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE", "Data/RTXCovidPneumonia/Test/V6")
PATH_TO_LOCAL_FILE = os.environ.get("GCP_GCS_PATH_TO_SAVED_FILE", "/tmp/data/dataset")

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'downlaod_file_gcp',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=10,
)

bucket_files_list = GoogleCloudStorageListOperator(
    task_id='bucket_file_list',
    bucket='covid-mlengine',
    prefix='Data/RTXCovidPneumonia/Test/V6/')

print(bucket_files_list)
