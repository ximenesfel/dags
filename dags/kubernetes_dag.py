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

ports = k8s.V1ContainerPort(container_port=6006, host_port=6006)

exec_action = k8s.V1ExecAction(command=["/bin/bash", "-c", "pgrep python"])

probe = k8s.V1Probe(_exec=exec_action)

dag_run = dag.get_active_runs()
run_id = dag_run.run_id


training = k8s.V1Container(image="ximenesfel/mnist_training:latest", 
                           command=["python", "/root/code/fashion_mnist.py", "-f", f"{run_id}"], 
                           name="training",
                           tty=True,
                           volume_mounts=[volume_mount])

tensorboard = k8s.V1Container(image="ximenesfel/mnist_tensorboard:latest", 
                              command=["tensorboard", "--logdir",  "/root/tensorboard", "--bind_all"],
                              name="tensorboard",
                              tty=True,
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

# tensorboard = KubernetesPodOperator(
#     namespace='airflow',
#     image="ximenesfel/mnist_tensorboard:latest",
#     cmds=["tensorboard",  "--logdir",  "/root/tensorboard", "--bind_all"],
#     name="tensorboard",
#     in_cluster=True,
#     task_id="tensorboard",
#     volumes=[volume],
#     volume_mounts=[volume_mount],
#     is_delete_operator_pod=True,
#     startup_timeout_seconds=300,
#     ports=[ports],
#     get_logs=True,
#     dag=dag
# )

# training = KubernetesPodOperator(
#     namespace='airflow',
#     image="ximenesfel/mnist_training:latest",
#     cmds=["python", "/root/code/fashion_mnist.py"],
#     name="training",
#     in_cluster=True,
#     task_id="training",
#     is_delete_operator_pod=True,
#     volumes=[volume],
#     volume_mounts=[volume_mount],
#     startup_timeout_seconds=300,
#     get_logs=True,
#     dag=dag
# )

training = KubernetesPodOperator(
    namespace='airflow',
    name="training",
    in_cluster=True,
    task_id="training",
    is_delete_operator_pod=True,
    startup_timeout_seconds=300,
    labels={"app": "tensorboard"},
    full_pod_spec=pod,
    get_logs=True,
    dag=dag
)

# def sucess_tensorboard_task():
#     dag_id = 'kubernetes_training'
#     dag_runs = DagRun.find(dag_id=dag_id)
#     for dag_run in dag_runs:
#         task = dag_run.get_task_instance("tensorboard")
#         print(task)
#         print(f"Actual task state: {task.state}")
#         task.state = State.SUCCESS
#         print(f"Modified task state: {task.state}")

# finish = PythonOperator(
#     task_id='finish',
#     python_callable=sucess_tensorboard_task,
#     trigger_rule="one_success",
#     dag=dag
# )

finish = BashOperator(
    task_id='finish',
    bash_command='echo 1',
    dag=dag
)

#start >> [tensorboard,training] >> finish
start >> training >> finish