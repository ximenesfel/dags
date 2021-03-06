from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils.dates import days_ago
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
    's3_download_dataset',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=10,
    tags=['training'],
)

volume_mount = k8s.V1VolumeMount(
    name='dataset', mount_path='/root/dataset', sub_path=None)

volume = k8s.V1Volume(
    name='dataset',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='dataset-claim'),
)

AWS_ACCESS_KEY_ID = Secret(
   deploy_type="env", deploy_target="AWS_ACCESS_KEY_ID", secret="aws", key="AWS_ACCESS_KEY_ID"
)

AWS_DEFAULT_REGION = Secret(
   deploy_type="env", deploy_target="AWS_DEFAULT_REGION", secret="aws", key="AWS_DEFAULT_REGION"
)

AWS_SECRET_ACCESS_KEY = Secret(
   deploy_type="env", deploy_target="AWS_SECRET_ACCESS_KEY", secret="aws", key="AWS_SECRET_ACCESS_KEY"
)

start = BashOperator(
    task_id='start',
    bash_command='echo 1',
    dag=dag
)

download_dataset = KubernetesPodOperator(
    namespace='airflow',
    image="ximenesfel/aws_s3:latest",
    cmds = ["python", "/root/code/download_file_from_s3_bucket.py", "-p", "{{ dag_run.conf['s3_bucket'] }}", "-i"],
    arguments=['{{ run_id }}'],
    name="dataset",
    in_cluster=True,
    task_id="dataset",
    volumes=[volume],
    secrets=[AWS_ACCESS_KEY_ID, AWS_DEFAULT_REGION, AWS_SECRET_ACCESS_KEY],
    volume_mounts=[volume_mount],
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