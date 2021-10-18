import os

from airflow import models
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from kubernetes.client import models as k8s

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

GCP_JSON_PATH = Secret(
   deploy_type="env", deploy_target="GOOGLE_APPLICATION_CREDENTIALS", secret="gcp", key="GOOGLE_APPLICATION_CREDENTIALS"
)

volume_mount_dataset = k8s.V1VolumeMount(
    name='dataset', mount_path='/root/dataset', sub_path=None)

volume_dataset = k8s.V1Volume(
    name='dataset',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='dataset-claim'),
)

volume_mount_credentials = k8s.V1VolumeMount(
    name='credentials', mount_path='/root/credentials', sub_path=None)

volume_credentials = k8s.V1Volume(
    name='credentials',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='credentials-claim'),
)

start = BashOperator(
    task_id='start',
    bash_command='echo 1',
    dag=dag
)

download_dataset = KubernetesPodOperator(
    namespace='airflow',
    image="gcr.io/tele-covid19/download_dataset:latest",
    cmds = ["python", "/root/code/gcp.py", "-p", "test", "-i"],
    arguments=['{{ run_id }}'],
    name="dataset",
    in_cluster=True,
    task_id="dataset",
    volumes=[volume_dataset, volume_credentials],
    secrets=[GCP_JSON_PATH],
    volume_mounts=[volume_mount_dataset, volume_mount_credentials],
    image_pull_secrets=[k8s.V1LocalObjectReference('gcr')],
    is_delete_operator_pod=True,
    startup_timeout_seconds=300,
    do_xcom_push=True,
    get_logs=True,
    dag=dag
)

finish = BashOperator(
    task_id='finish',
    bash_command='echo 1',
    dag=dag
)

start >> download_dataset >> finish