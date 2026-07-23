"""Daily Customer 360 DAG.

The same public CLI used by local development and CI is orchestrated here.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="customer360_daily_lakehouse",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["customer360", "pyspark", "lakehouse"],
) as dag:
    generate = BashOperator(
        task_id="generate_or_land_partition",
        bash_command=(
            "customer360 generate --root ${C360_DATA_ROOT:-/opt/project/data} "
            "--start-date {{ ds }} --days 1"
        ),
    )
    transform = BashOperator(
        task_id="bronze_silver_gold",
        bash_command=(
            "customer360 run --root ${C360_DATA_ROOT:-/opt/project/data} "
            "--start-date {{ ds }} --days 1 --mode incremental"
        ),
    )
    generate >> transform
