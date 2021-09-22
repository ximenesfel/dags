from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
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
    'tensorboard',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=10,
    tags=['training'],
)

volume_mount = k8s.V1VolumeMount(
    name='tensorboard', mount_path='/root/tensorboard', sub_path=None)

volume = k8s.V1Volume(
    name='tensorboard',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='tensorboard-claim'),
)

ports = k8s.V1ContainerPort(container_port=6006, host_port=6006)

start = BashOperator(
    task_id='start',
    bash_command='echo 1',
    dag=dag
)

tensorboard = KubernetesPodOperator(
    namespace='airflow',
    image="ximenesfel/mnist_tensorboard:latest",
    cmds=["tensorboard",  "--logdir",  "/root/tensorboard", "--bind_all"],
    name="tensorboard",
    in_cluster=True,
    task_id="tensorboard",
    volumes=[volume],
    volume_mounts=[volume_mount],
    is_delete_operator_pod=True,
    startup_timeout_seconds=300,
    ports=[ports],
    get_logs=True,
    dag=dag
)


finish = BashOperator(
    task_id='finish',
    bash_command='echo 1',
    dag=dag
)

#start >> [tensorboard,training] >> finish
start >> tensorboard >> finish