from airflow import DAG
from airflow.sdk import Asset
from pendulum import datetime
from smooth_operators import IngestS3Operator

SOURCE_DATASET = Asset("s3://source_s3/customers.csv")
TARGET_DATASET = Asset("s3://raw-bucket/sys-b-customers-{{ ds }}.csv")

with DAG(
    dag_id="ingest_customers",
    start_date=datetime(2023, 1, 1),
    schedule="1 0 * * *",
    catchup=False,
) as dag:

    def ingest():
        print("Ingesting customers...")

    ingest_task = IngestS3Operator(
        task_id="ingest_task",
        source_path=SOURCE_DATASET,
        target_path=TARGET_DATASET,
        outlets=[TARGET_DATASET],
    )

    ingest_task
