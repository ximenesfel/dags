# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import os

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.utils.dates import days_ago

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'testairflowkuberntes')


@task(task_id="s3_bucket_dag_add_keys_to_bucket")
def upload_keys():
    """This is a python callback to add keys into the s3 bucket"""
    # add keys to bucket
    s3_hook = S3Hook()
    for i in range(0, 3):
        s3_hook.load_string(
            string_data="input",
            key=f"path/data{i}",
            bucket_name=BUCKET_NAME,
        )


with DAG(
    dag_id='s3_bucket_dag',
    schedule_interval=None,
    start_date=days_ago(2),
    default_args={"bucket_name": BUCKET_NAME},
    max_active_runs=1,
    tags=['example'],
) as dag:

    # [START howto_operator_s3_bucket]
    create_bucket = S3CreateBucketOperator(
        task_id='s3_bucket_dag_create',
        aws_conn_id="aws",
        region_name='us-east-1',
    )

    # Using a task-decorated function to add keys
    add_keys_to_bucket = upload_keys()

    delete_bucket = S3DeleteBucketOperator(
        task_id='s3_bucket_dag_delete',
        aws_conn_id="aws",
        force_delete=True,
    )
    # [END howto_operator_s3_bucket]

    create_bucket >> add_keys_to_bucket >> delete_bucket