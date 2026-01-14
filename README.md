# End-to-End AWS Spark Data Pipeline

This project demonstrates a production-style **data engineering pipeline**
built using **PySpark, Airflow, and AWS services**.

## Architecture Overview
- Source Systems:
  - AWS S3 (customer migration data)
  - Snowflake (customer, purchase, violation tables)
  - Web API (current customer location)
- Processing:
  - PySpark ETL jobs on EMR
  - Intermediate data stored in S3 (Parquet format)
  - Final aggregation via master Spark job
- Orchestration:
  - Airflow DAG for job sequencing, retries, and scheduling
- Target:
  - Curated datasets in S3
  - OpenSearch integration (in progress)

## Tech Stack
- PySpark, Spark SQL
- AWS S3, EMR, EC2
- Apache Airflow
- Snowflake
- GitHub

## Project Structure
jobs/
extract_s3.py
extract_snowflake.py
extract_webapi.py
master_job.py

airflow/
data_pipeline_dag.py
## Key Highlights
- TB-scale data processing
- Performance tuning (broadcast joins, filtering, column pruning)
- Secure credential handling via Secrets Manager (conceptual)
- Daily scheduled batch pipeline

> Note: Sample paths and placeholders are used for demonstration.

