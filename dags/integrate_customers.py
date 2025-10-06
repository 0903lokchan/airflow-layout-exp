from airflow import DAG
from airflow.sdk import Asset
from pendulum import datetime

SOURCE_DATASETS = {
    "sys_a": Asset("s3://raw-bucket/sys-a-customers-{{ ds }}.csv"),
    "sys_b": Asset("s3://raw-bucket/sys-b-customers-{{ ds }}.csv"),
}
TARGET_DATASET = Asset("s3://processed-bucket/customers")
WORKING_CACHE = "s3://working_bucket/{{ dag.dag_id }}/{{ dag_run.run_id }}"

with DAG(
    dag_id="integrate_customers",
    start_date=datetime(2023, 1, 1),
    schedule="1 0 * * *",
    catchup=False,
) as dag:
    standardise_tasks = []
    for system, source in SOURCE_DATASETS.items():
        task = StandardiseCSVOperator(
            task_id=f"standardise_{system}_task",
            source_path=source,
            target_path=WORKING_CACHE + ,
        )

    integrate_customers = SmoothK8sOperator(
        task_id="integrate_task",
        python_callable=integrate,
    )

    integrate_task