# End-to-End AWS Data Pipeline

This project implements a scalable data pipeline built on AWS
using **PySpark and Airflow**.

## Pipeline Overview
- Source systems: S3, Snowflake, Web API
- PySpark jobs perform extraction and transformations
- Intermediate and final data stored in AWS S3
- Orchestration handled using Airflow DAGs

## Tech Stack
- PySpark, Spark SQL
- AWS S3, EMR, EC2
- Apache Airflow
- Snowflake

## Project Structure
jobs/
airflow/
configs/

> Note: Sample paths and configurations are used for demonstration.
