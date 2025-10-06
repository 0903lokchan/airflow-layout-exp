# Printing Press

A proof-of-concept dev tool for generating boilerplate Airflow DAGs from templates.

## Concept

This tool promotes a "build-time" DAG generation strategy over "run-time" dynamic DAGs. Instead of the Airflow scheduler parsing Python code that dynamically generates DAGs, `printing-press` pre-renders static, self-contained DAG files from a minimal set of configuration parameters.

This approach offers several advantages:

*   **Reduced Scheduler Overhead**: The scheduler parses simple, static files, leading to faster DAG parsing times and lower resource consumption.
*   **Improved Readability & Linting**: Generated DAGs are explicit and easier for developers and static analysis tools to understand.
*   **Standardization**: Enforces common DAG patterns (e.g., ingest, integrate) across the project.

## Usage

The tool provides commands to scaffold different types of DAGs.

### Create an Integration DAG

```bash
printing-press create integrate --dag-id integrate_customers --target-asset "s3://processed-bucket/customers" --image "your-docker-registry/integrate-customers:latest" -o dags/
```

This will generate a new DAG file `dags/integrate_customers.py` based on the integration template.

