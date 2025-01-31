from airflow import DAG
from airflow.operators.gcp_transfer_operator import GCSToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from datetime import datetime, timedelta

PROJECT_ID = "ttc-gcp"
REGION = "us-central1"
CLUSTER_NAME = "airflowcluster"
JOB_FILE_URL = "gs://codescripts/trans_code.py"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "secondary_worker_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 32,
        },
        "is_preemptible": True,
        "preemptibility": "PREEMPTIBLE",
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_FILE_URL},
}

default_args = {
    'owner': 'dilip',
    'depends_on_past': False,
    'email': ['etl.repdilip@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('gcs_to_gcs_dag',
          default_args=default_args,
          description='A simple DAG to transfer files from one GCS bucket to another',
          schedule_interval=timedelta(days=1),
          start_date=datetime(2025, 28, 1),
          catchup=False)

# loading source to target
t1 = GCSToGCSOperator(
    task_id='copy_gcs_to_gcs_task',
    source_bucket='source-system-dataset',
    source_object='Traffic_Collisions_Open_Data_3719442797094142699.csv',
    destination_bucket='toronto-traffic-ds-landing',
    destination_object='target/',
    move_object=False,  # Change to True if you want to move instead of copy
    replace=True,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# creating dataproc cluster
t2 = DataprocCreateClusterOperator(
    task_id="create_cluster_task",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
)

# pyspark script
t3 = DataprocSubmitJobOperator(
    task_id="pyspark_task",
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID
)

t4 = DataprocDeleteClusterOperator(
    task_id="delete_cluster_task",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
)

# running the tasks
t1 >> t2 >> t3 >> t4
