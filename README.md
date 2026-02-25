<img width="1408" height="752" alt="Gemini_Generated_Image_omckfnomckfnomck" src="https://github.com/user-attachments/assets/63ab762a-a1c8-4edd-9292-833c58cf253e" />


# Local Data Lakehouse Architecture (AWS LocalStack + Open Source)



## Overview
This project demonstrates an end-to-end, event-driven Data Lakehouse architecture built entirely on a local machine. It simulates a production AWS environment using **LocalStack** and integrates it with industry-standard open-source data tools to handle data ingestion, processing, storage, and visualization.

## Architecture Workflow
The data pipeline follows a highly decoupled, event-driven flow:

1. **Data Generation & Ingestion:** A Python script generates dummy data and uploads it as a file to a "Landing" S3 bucket.
2. **Event-Driven Trigger:** The S3 upload triggers an **S3 Event Notification**, which automatically invokes an **AWS Lambda** function.
3. **Job Orchestration:** The Lambda function catches the event payload and sends a `POST` request to a lightweight Flask server running inside our Spark container.
4. **Data Processing (Spark):** The Flask server triggers a `spark-submit` job. Apache Spark reads the raw file from the S3 landing bucket, enriches the data, and writes it back to a separate "Warehouse" S3 bucket in the **Apache Iceberg** table format.
5. **Metadata Management:** As Spark writes the data, it registers the table schema and metadata with a **Hive Metastore** (backed by a PostgreSQL database). 
6. **Query Engine (Trino):** **Trino** acts as the distributed SQL query engine. It connects to the Hive Metastore to understand the Iceberg table structure and reads the physical Parquet files directly from S3.
7. **Visualization (Metabase):** Finally, **Metabase** connects to Trino to serve as the BI layer, allowing us to build dashboards and query the enriched data using standard SQL.

## Infrastructure as Code & Open Source Alternatives
All AWS infrastructure is provisioned automatically using **Terraform**. The Terraform scripts handle the creation of:
* S3 Buckets (Landing and Warehouse)
* Lambda function packaging (uploading the `.zip` to S3)
* IAM Roles and Policies (allowing S3 to invoke Lambda, and Lambda to write to CloudWatch)
* S3 Event Notifications
* CloudWatch Log Groups

### The "LocalStack Free" Workaround
In a real-world, fully managed AWS environment, the data processing and analytics layers would typically use managed AWS services. However, because services like EMR, Glue, and Athena are not available in the **LocalStack Free tier**, this project replaces them with their direct open-source counterparts running in Docker containers:

| Managed AWS Service | Open-Source Equivalent Used Here | Role in Architecture |
| :--- | :--- | :--- |
| **Amazon EMR** | **Apache Spark** | Distributed data processing and transformation. |
| **AWS Glue Catalog** | **Hive Metastore + Postgres** | Centralized metadata repository and table catalog. |
| **Amazon Athena** | **Trino** | Serverless SQL query engine for data in S3. |
| **Amazon QuickSight** | **Metabase** | Business Intelligence and visualization dashboard. |

This hybrid approach allows you to develop and test a robust, AWS-native data pipeline locally without incurring cloud costs or requiring a paid LocalStack license.
