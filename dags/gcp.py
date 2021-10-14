import os

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.dates import days_ago
from airflow import DAG

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "tele-covid19")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "covid-mlengine")

PATH_TO_REMOTE_FILE = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE", "Data/RTXCovidPneumonia/Test/V6")
PATH_TO_LOCAL_FILE = os.environ.get("GCP_GCS_PATH_TO_SAVED_FILE", "/tmp/data/dataset")


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
