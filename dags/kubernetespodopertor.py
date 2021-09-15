from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5),
    'email': ['airflow@my_first_dag.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

my_first_dag = DAG(
    'kubernetespodoperator',
    default_args=default_args,
    description='Our first DAG',
    schedule_interval=timedelta(days=1),
)

task_1 = KubernetesPodOperator(
    task_id='kubernetes',
    pod_template_file='./pod/multicontainer-pod.yaml',
    dag=my_first_dag,
)

task_2 = BashOperator(
    task_id='second_task',
    bash_command='echo 2',
    dag=my_first_dag,
)

task_1.set_downstream(task_2)