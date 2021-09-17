from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

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

init_container = k8s.V1Container(
    name="init-container",
    image="tensorflow/tensorflow:2.4.2",
    command=["tensorboard", "--help"],
)

org_node = KubernetesPodOperator(
    namespace='airflow',
    image="ximenesfel/mnist_training:latest",
    cmds=["python", "/root/code/fashion_mnist.py"],
    name="training",
    in_cluster=True,
    task_id="training",
    is_delete_operator_pod=True,
    startup_timeout_seconds=300,
    init_containers=[init_container],
    get_logs=True,
    dag=dag
)

org_node