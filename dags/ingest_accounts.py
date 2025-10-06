from airflow import DAG
from airflow.sdk import Asset
from pendulum import datetime
from smooth_operators import IngestPostgresOperator

SOURCE_DATASET = Asset("postgres://source_db/accounts")
TARGET_DATASET = Asset("s3://raw-bucket/accounts.parquet")

with DAG(
    dag_id="ingest_accounts",
    start_date=datetime(2023, 1, 1),
    schedule="1 0 * * *",
    catchup=False,
) as dag:

    def ingest():
        print("Ingesting customers...")

    ingest_task = IngestPostgresOperator(
        task_id="ingest_task",
        conn_id="postgres_account_db",
        source_query="SELECT * FROM accounts;",
        target_path=TARGET_DATASET,
        target_format="parquet",
        outlets=[TARGET_DATASET],
    )

    ingest_task
