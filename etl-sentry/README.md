# ETL Sentry Module

The **ETL Sentry** module is a data validation and quality monitoring pipeline designed to ensure that 
data in a BigQuery warehouse adheres to predefined quality standards. 
The module leverages the [Deequ](https://github.com/awslabs/deequ) library for data quality checks and 
provides utilities for generating data validation reports, constraint suggestions, and notifications 
for quality issues.

## Table of Contents
- [Requirements](#requirements)
- [Features](#features)
- [Usage](#usage)
  - [Unit Test](#unit-test)
  - [Configuration](#configuration)
  - [Submit Validation Job](#submit-a-validation-job-to-google-cloud-dataproc)
  - [Submit Constraint Suggestion Job](#submit-a-constraint-suggestion-job-to-google-cloud-dataproc)

## Requirements

- **Java Version**: 8 or higher
- **Build Tool**: Maven or Gradle
- **Dependencies:**
  - AWS Deequ SDK (`com.amazon.deequ`)
  - Google Cloud BigQuery SDK (`com.google.cloud:google-cloud-bigquery`)

## Features
- **Data Validation Pipeline**
  Validates BigQuery data against predefined quality checks and stores results in a BigQuery repository.

- **Constraint Suggestion Pipeline**
  Analyzes BigQuery data and generates constraint suggestions for improving data quality rules.

- **Data Quality Notifications**
  Sends notifications when data quality issues are detected.

- **BigQuery Integration**
  Reads from and writes validation results to BigQuery tables.

## Usage

### Unit Test

To run unit tests, execute the following command.

```shell
./gradlew clean test
```

### Configuration

Configuration is managed through JSON files. Create a configuration file (e.g., config.json) and store it in GCS
with the following structure.

```json
{
  "group_name": "bronze-daily",
  "max_threads": 2,
  "config_bucket": "gs://github-config-staging",
  "deequ_repository_config": {
    "project_id": "github-staging",
    "region": "asia-southeast1",
    "dataset_id": "bronze_sentry",
    "create_disposition": "CREATE_IF_NEEDED",
    "temporary_gcs_bucket": "gs://dataproc-temp-staging"
  },
  "source_bigquery_config": {
    "project_id": "github-staging",
    "region": "asia-southeast1"
  },
  "table_whitelist": [
    "bronze_daily_mysql.github_staging_orders",
    "bronze_daily_postgresql.github_staging_users",
    "bronze_daily_mongodb.github_staging_transactions"
  ]
}
```

### Submit a validation job to Google Cloud Dataproc

To submit a validation job, execute the following command.

```shell
gcloud dataproc jobs submit spark \
--project='<your-project-id>' \
--cluster='<your-cluster-name>' \
--region='<your-region>' \
--class='com.github.davidch93.etl.sentry.EtlSentryApplication' \
--jars='gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.41.0.jar,build/libs/etl-sentry.jar' \
--properties='^;^spark.sql.adaptive.enabled=true;spark.yarn.appMasterEnv.SMTP_HOST=smtp.mailgun.org;spark.yarn.appMasterEnv.SMTP_PORT=587;spark.yarn.appMasterEnv.SMTP_STARTTLS=False;spark.yarn.appMasterEnv.SMTP_SSL=False;spark.yarn.appMasterEnv.SMTP_MAIL_SENDER=cortabot@bukalapak.com;spark.yarn.appMasterEnv.SMTP_PASSWORD=<base64encoded-password>;spark.yarn.appMasterEnv.SMTP_USER=databot@bukalapak.com;spark.yarn.appMasterEnv.MAILING_LIST=<your-email>' \
-- \
validate \
--config-path <your-filesystem>://config.json \
--scheduled-timestamp <your-start-timestamp>
```

### Submit a constraint suggestion job to Google Cloud Dataproc

To submit a constraint suggestion job, execute the following command.

```shell
gcloud dataproc jobs submit spark \
--project='<your-project-id>' \
--cluster='<your-cluster-name>' \
--region='<your-region>' \
--class='com.github.davidch93.etl.sentry.EtlSentryApplication' \
--jars='gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.41.0.jar,build/libs/etl-sentry.jar' \
--properties='^;^spark.sql.adaptive.enabled=true;spark.yarn.appMasterEnv.SMTP_HOST=smtp.mailgun.org;spark.yarn.appMasterEnv.SMTP_PORT=587;spark.yarn.appMasterEnv.SMTP_STARTTLS=False;spark.yarn.appMasterEnv.SMTP_SSL=False;spark.yarn.appMasterEnv.SMTP_MAIL_SENDER=cortabot@bukalapak.com;spark.yarn.appMasterEnv.SMTP_PASSWORD=<base64encoded-password>;spark.yarn.appMasterEnv.SMTP_USER=databot@bukalapak.com;spark.yarn.appMasterEnv.MAILING_LIST=<your-email>' \
-- \
suggest_constraint_rules \
--bigquery-table-name <your-bigquery-table>
```
