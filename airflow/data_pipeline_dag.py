from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10)
}

with DAG(
    dag_id="aws_spark_data_pipeline",
    default_args=default_args,
    description="End-to-end AWS Spark data pipeline",
    schedule_interval="0 9 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    create_cluster = BashOperator(
        task_id="create_emr_cluster",
        bash_command="echo 'Creating EMR cluster'"
    )

    s3_extraction = BashOperator(
        task_id="s3_extraction_job",
        bash_command="echo 'Running S3 PySpark job'"
    )

    snowflake_extraction = BashOperator(
        task_id="snowflake_extraction_job",
        bash_command="echo 'Running Snowflake PySpark job'"
    )

    webapi_extraction = BashOperator(
        task_id="webapi_extraction_job",
        bash_command="echo 'Running Web API PySpark job'"
    )

    master_job = BashOperator(
        task_id="master_aggregation_job",
        bash_command="echo 'Running Master PySpark job'"
    )

    terminate_cluster = BashOperator(
        task_id="terminate_emr_cluster",
        bash_command="echo 'Terminating EMR cluster'"
    )

    create_cluster >> s3_extraction >> snowflake_extraction >> webapi_extraction >> master_job >> terminate_cluster
