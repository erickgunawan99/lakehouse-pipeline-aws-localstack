terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

 
# Provider (LocalStack)
 
provider "aws" {
  region     = "us-east-1"
  access_key = "test"
  secret_key = "test"

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  s3_use_path_style = true 
  endpoints {
    s3     = "http://localhost:4566"
    lambda = "http://localhost:4566"
    iam    = "http://localhost:4566"
  }
}

 
# Locals
 
locals {
  lakehouse_bucket = "lakehouse-warehouse"
}

 
# S3 Bucket (explicitly managed)
 
resource "aws_s3_bucket" "lakehouse" {
  bucket        = local.lakehouse_bucket
  force_destroy = true
}


 
# Lambda role
# assume role defines who can use the role
 
resource "aws_iam_role" "lambda_role" {
  name = "lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# 2. The Policy
# inline policy defines what the role can do  
resource "aws_iam_role_policy" "lambda_logging" {
  name = "lambda-logging-policy"
  role = aws_iam_role.lambda_role.id # This links the policy to the role above

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.lambda_logs.arn}:*"
      }
    ]
  })
}

 
# Upload Lambda ZIP to S3
 
resource "aws_s3_object" "lambda_zip" {
  bucket = aws_s3_bucket.lakehouse.bucket
  key    = "lambda/lambda.zip"
  source = "lambda.zip"
  source_hash = filebase64sha256("lambda.zip")
}

 
# Lambda function
 
resource "aws_lambda_function" "handler" {
  function_name = "iceberg-event-handler"
  runtime       = "python3.10"
  handler       = "lambda.handler"
  timeout       = 30
  role          = aws_iam_role.lambda_role.arn

  s3_bucket        = aws_s3_bucket.lakehouse.bucket
  s3_key           = aws_s3_object.lambda_zip.key
  source_code_hash = filebase64sha256("lambda.zip")
}

# Define the log group explicitly
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.handler.function_name}"
  retention_in_days = 7
}


 
# Allow S3 to invoke Lambda
 
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.handler.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.lakehouse.arn
}


 
# S3 → Lambda notification
 
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.lakehouse.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.handler.arn
    events              = ["s3:ObjectCreated:*"]
    
    # This ensures only your specific data files trigger the Spark job
    filter_prefix       = "data/user_events_"
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

# ==============================================================================
# AWS CLOUD-ONLY RESOURCES (FOR LEARNING PURPOSES ONLY WONT BE PROVISIONED BY terraform apply)
# Note: These will not work in LocalStack Free. Use 'terraform plan' to see 
# how they would look without actually creating them in a real account.
# ==============================================================================

/*

 
# Networking & Security
 

# The "Home" for your Spark jobs
resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"
}

# Spark workers live in Private subnets (Best Practice)
resource "aws_subnet" "private" {
  vpc_id     = aws_vpc.main.id
  cidr_block = "10.0.1.0/24"
}

# This is the "Door" that lets Spark talk to S3 without going over the internet
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.main.id
  service_name = "com.amazonaws.us-east-1.s3"
  route_table_ids = [aws_vpc.main.main_route_table_id]
}

 
# 2. AWS Glue Catalog (Iceberg Table Definition)
# In AWS, this replaces your manual 'CREATE TABLE' Spark commands
 

resource "aws_glue_catalog_database" "iceberg_db" {
  name = "lakehouse_db"
}

resource "aws_glue_catalog_table" "user_events" {
  name          = "user_events"
  database_name = aws_glue_catalog_database.iceberg_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "table_type"      = "ICEBERG"
    "format"          = "parquet"
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse/user_events/"
  }
}

 
# 3. Amazon EMR Serverless (Modern Spark Execution)
# This replaces your 'docker-compose' Spark cluster.
 

resource "aws_emrserverless_application" "spark_app" {
  name          = "iceberg-spark-app"
  release_label = "emr-6.10.0"
  type          = "spark"

  initial_capacity {
    initial_capacity_type = "Driver"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "4 GB"
      }
    }
  }
  network_configuration {
    subnet_ids         = [aws_subnet.private.id]
    security_group_ids = [aws_security_group.allow_internal.id]
    }
}

 
# Updated Spark Execution Role
  When you call the AWS API to start a job (e.g., via your Lambda or a script), 
  you pass executionRoleArn = aws_iam_role.spark_execution.arn.
 

resource "aws_iam_role" "spark_execution" {
  name = "spark-execution-role"

  # Crucial: EMR Serverless must be allowed to assume this role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { 
        Service = "emr-serverless.amazonaws.com" # <--- Must be emr-serverless
      }
      Action = "sts:AssumeRole"
    }]
  })
}

# This policy gives your Spark code the "Keys" to the Lakehouse
resource "aws_iam_role_policy" "spark_policy" {
  name = "spark-access-policy"
  role = aws_iam_role.spark_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = ["arn:aws:s3:::${local.lakehouse_bucket}", "arn:aws:s3:::${local.lakehouse_bucket}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:CreateTable", "glue:GetTable", "glue:UpdateTable"]
        Resource = ["*"] # In production, scope this to your Glue DB ARN
      }
    ]
  })
}

 
# 4. Amazon Athena (SQL Interface)
# This allows you to query S3 Parquet/Iceberg files using SQL.
 

resource "aws_athena_workgroup" "lakehouse_wg" {
  name = "lakehouse-analytics"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"
    }
  }
}

 
# 5. Amazon QuickSight (BI Dashboarding)
# Note: QuickSight requires manual account activation in the AWS Console first.
 

resource "aws_quicksight_data_source" "athena_source" {
  data_source_id = "athena-lakehouse-source"
  name           = "AthenaLakehouse"
  type           = "ATHENA"

  parameters {
    athena {
      work_group = aws_athena_workgroup.lakehouse_wg.name
    }
  }
}

 
# 6. Additional Security Groups
 

resource "aws_security_group" "allow_internal" {
  name        = "allow_internal_spark"
  description = "Allow EMR workers to talk to each other"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }
}

*/