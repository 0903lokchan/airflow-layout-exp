from airflow import DAG
from airflow.sdk import Asset
from pendulum import datetime
from smooth_operators import SmoothK8sOperator, StandardiseCSVOperator

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
            target_path=f"{WORKING_CACHE}/{system}_standardised.csv",
        )
        standardise_tasks.append(task)

    integrate_customers = SmoothK8sOperator(
        task_id="integrate_task",
        # python_callable=integrate, # This parameter is not valid for KubernetesPodOperator
        # ... other k8s parameters like image, cmds, etc.
        image="your-docker-registry/integrate-customers:latest", # Specify your integration image
        # Example arguments passing the location of standardised files
        arguments=[
            "--input-a", f"{WORKING_CACHE}/sys_a_standardised.csv",
            "--input-b", f"{WORKING_CACHE}/sys_b_standardised.csv",
            "--output", TARGET_DATASET.uri,
        ],
        outlets=[TARGET_DATASET],
    )

    # You will need to define task dependencies here, e.g.:
    # standardise_tasks >> integrate_customers
    standardise_tasks >> integrate_customers
