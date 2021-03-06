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
    'kubernetes_training',
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

exec_action = k8s.V1ExecAction(command=["/bin/bash", "-c", "pgrep python"])

probe = k8s.V1Probe(_exec=exec_action)

env_variable = k8s.V1EnvVar(name="run_id", value='{{ run_id }}')

training = k8s.V1Container(image="ximenesfel/mnist_training:latest", 
                           command=["python", "/root/code/fashion_mnist.py", "-f"],
                           args=['{{ run_id }}'],
                           name="training",
                           tty=True,
                           volume_mounts=[volume_mount])

tensorboard = k8s.V1Container(image="ximenesfel/mnist_tensorboard:latest", 
                              command=["tensorboard", "--bind_all", "--logdir", "/root/tensorboard/$run_id"],
                              #args=["/root/tensorboard/" + "{{ run_id }}"],
                              name="tensorboard",
                              tty=True,
                              env=[env_variable],
                              liveness_probe=probe,
                              ports=[ports],
                              volume_mounts=[volume_mount])

pod_spec = k8s.V1PodSpec(containers=[training, tensorboard],
                         volumes=[volume],
                         restart_policy="Never",
                         share_process_namespace=True)

pod = k8s.V1Pod(spec=pod_spec)

start = BashOperator(
    task_id='start',
    bash_command='echo 1',
    dag=dag
)

training = KubernetesPodOperator(
    namespace='airflow',
    name="training",
    in_cluster=True,
    task_id="training",
    is_delete_operator_pod=True,
    startup_timeout_seconds=300,
    env_vars=[env_variable],
    arguments=['{{ run_id }}'],
    full_pod_spec=pod,
    get_logs=True,
    dag=dag
)

finish = BashOperator(
    task_id='finish',
    bash_command='echo 1',
    dag=dag
)

start >> training >> finish