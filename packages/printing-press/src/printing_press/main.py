import typer
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

app = typer.Typer()
templates_path = Path(__file__).parent / "templates"
env = Environment(loader=FileSystemLoader(templates_path))

@app.command()
def hello(name: str):
    print(f"Hello {name}")

create_app = typer.Typer()
app.add_typer(create_app, name="create")

@create_app.command("ingest")
def create_ingest(
    dag_id: str,
    source_asset: str,
    target_asset: str,
    conn_id: str,
    source_query: str,
    target_format: str,
    output_dir: Path = typer.Option(Path.cwd(), "--output-dir", "-o"),
    start_date: str = "2023, 1, 1",
    schedule: str = "1 0 * * *",
):
    template = env.get_template("ingest.py.template")
    content = template.render(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        source_asset=source_asset,
        target_asset=target_asset,
        conn_id=conn_id,
        source_query=source_query,
        target_format=target_format,
    )
    output_file = output_dir / f"{dag_id}.py"
    output_file.write_text(content)
    print(f"Created DAG {output_file}")

@create_app.command("integrate")
def create_integrate(
    dag_id: str,
    target_asset: str,
    image: str,
    output_dir: Path = typer.Option(Path.cwd(), "--output-dir", "-o"),
    start_date: str = "2023, 1, 1",
    schedule: str = "1 0 * * *",
):
    # For simplicity, we'll hardcode the source assets and arguments for now
    source_assets = {
        "sys_a": "s3://raw-bucket/sys-a-customers-{{ ds }}.csv",
        "sys_b": "s3://raw-bucket/sys-b-customers-{{ ds }}.csv",
    }
    arguments = [
        "--input-a", f"s3://working_bucket/{dag_id}/{{{{ dag_run.run_id }}}}/sys_a_standardised.csv",
        "--input-b", f"s3://working_bucket/{dag_id}/{{{{ dag_run.run_id }}}}/sys_b_standardised.csv",
        "--output", target_asset,
    ]
    template = env.get_template("integrate.py.template")
    content = template.render(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        source_assets=source_assets,
        target_asset=target_asset,
        image=image,
        arguments=arguments,
    )
    output_file = output_dir / f"{dag_id}.py"
    output_file.write_text(content)
    print(f"Created DAG {output_file}")

if __name__ == "__main__":
    app()