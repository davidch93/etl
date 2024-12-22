# etl-batch Module

The `etl-batch` module is designed to manage batch data pipelines, enabling efficient CDC data extraction,
transformation, and loading (ETL) processes.
It leverages Apache Spark for data processing, integrates with Kafka for batch data ingestion, and uses Google BigQuery
for storage and analytics.

## Table of Contents

- [Requirements](#requirements)
- [Features](#features)
- [Usage](#usage)
    - [Unit Test](#unit-test)
    - [Configuration](#configuration)
    - [Submit Data Pool Job](#submit-a-data-pool-job-to-google-cloud-dataproc)

## Requirements

- **Java Version**: 8 or higher
- **Build Tool**: Maven or Gradle
- **Dependencies:**
    - Apache Spark with Kafka integration SDK
    - Google Cloud Dataproc SDK
    - Google Cloud BigQuery SDK (`com.google.cloud:google-cloud-bigquery`)

## Features

- **Data Pool Pipeline**  
  Extract CDC data from Kafka topics with specified time ranges, transform data into raw data lake, curated data lake,
  and data pool layers in Google Cloud Storage (GCS).

- **BigQuery Integration**  
  Write processed data to BigQuery with configurable partitioning and table schema.

- **Error Handling and Notifications**  
  Logs and notifies users of failed jobs via email.

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
  "data_lake_bucket": "gs://github-data-lake-staging",
  "data_pool_bucket": "gs://github-data-pool-staging",
  "config_bucket": "gs://github-config-staging",
  "kafka_config": {
    "bootstrap_servers": "localhost:9092"
  },
  "bigquery_config": {
    "project_id": "github-staging",
    "region": "asia-southeast1",
    "dataset_id": "bronze",
    "create_disposition": "CREATE_IF_NEEDED",
    "partition_expiry_millis": 172800000,
    "temporary_gcs_bucket": "gs://dataproc-temp-staging"
  },
  "table_whitelist": [
    "mysqlstaging.github_staging.orders",
    "postgresqlstaging.github_staging.users",
    "mongodbstaging.github_staging.transactions"
  ]
}
```

### Submit a data pool job to Google Cloud Dataproc

To submit a data pool job, execute the following command.

```shell
gcloud dataproc jobs submit spark \
--project='<your-project-id>' \
--cluster='<your-cluster-name>' \
--region='<your-region>' \
--class='com.github.davidch93.etl.sentry.EtlSentryApplication' \
--jars='gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.41.0.jar,build/libs/etl-sentry.jar' \
--properties='^;^spark.sql.adaptive.enabled=true;spark.yarn.appMasterEnv.SMTP_HOST=smtp.mailgun.org;spark.yarn.appMasterEnv.SMTP_PORT=587;spark.yarn.appMasterEnv.SMTP_STARTTLS=False;spark.yarn.appMasterEnv.SMTP_SSL=False;spark.yarn.appMasterEnv.SMTP_MAIL_SENDER=cortabot@bukalapak.com;spark.yarn.appMasterEnv.SMTP_PASSWORD=<base64encoded-password>;spark.yarn.appMasterEnv.SMTP_USER=databot@bukalapak.com;spark.yarn.appMasterEnv.MAILING_LIST=<your-email>' \
-- \
create_data_pool \
--config-path <your-filesystem>://<path>/config.json \
--scheduled-timestamp <your-start-timestamp>
```
