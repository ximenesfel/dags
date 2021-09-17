from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
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
    'kubernetes_training',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=10,
    tags=['training'],
)

# Generate 2 tasks
# tasks = ["task{}".format(i) for i in range(1, 2)]
# example_dag_complete_node = DummyOperator(task_id="example_dag_complete", dag=dag)

# org_dags = []
# for task in tasks:

#     bash_command = 'echo HELLO'

volume_mount = k8s.V1VolumeMount(
    name='tensorboard', mount_path='/root/tensorboard', sub_path=None)

volume = k8s.V1Volume(
    name='tensorboard',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='tensorboard-claim'),
)

ports = k8s.V1ContainerPort(container_port=6006)

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

training = KubernetesPodOperator(
    namespace='airflow',
    image="ximenesfel/mnist_training:latest",
    cmds=["python", "/root/code/fashion_mnist.py"],
    name="training",
    in_cluster=True,
    task_id="training",
    is_delete_operator_pod=True,
    volumes=[volume],
    volume_mounts=[volume_mount],
    startup_timeout_seconds=300,
    get_logs=True,
    dag=dag
)

[tensorboard, training]