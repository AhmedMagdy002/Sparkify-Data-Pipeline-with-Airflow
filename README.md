# Sparkify-Data-Pipeline-with-Airflow
This project builds a data pipeline using **Apache Airflow** to automate the ETL process for Sparkify, a fictional music streaming company. The pipeline extracts log and song data from AWS S3, stages the data in Amazon Redshift, transforms it into a star schema, and performs data quality checks.

## Project Overview

Sparkify collects user activity and song metadata. The goal is to create a pipeline that:

- Automates the loading of raw JSON data from S3 into Redshift staging tables.
- Transforms and loads the data into fact and dimension tables.
- Ensures data integrity through SQL-based quality checks.

## Technologies Used

- **Apache Airflow** (workflow orchestration)
- **Amazon Redshift** (data warehouse)
- **Amazon S3** (data storage)
- **Python** (custom Airflow operators)
- **SQL** (data transformation and validation)

## DAG Workflow

The DAG consists of the following tasks:

1. **Begin Execution** – Dummy operator to start the DAG
2. **Stage Events** – Load event log data from S3 to Redshift
3. **Stage Songs** – Load song metadata from S3 to Redshift
4. **Load Fact Table** – Load data into the `songplays` fact table
5. **Load Dimension Tables**:
   - `users`
   - `songs`
   - `artists`
   - `time`
6. **Run Data Quality Checks** – Validate that all tables contain expected data
7. **Stop Execution** – Dummy operator to mark the DAG as complete

![DAG Diagram](https://github.com/user-attachments/assets/633c099f-886b-489d-b293-47f1af5c6289) 
