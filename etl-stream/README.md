# ETL Stream Module

The `etl-stream` module is a core component of the ETL (Extract, Transform, Load) pipeline, 
focused on streaming data transformations and processing for real-time systems. 
It integrates with Apache Beam and BigQuery to provide a robust framework for handling high-volume data streams.

## Table of Contents
- [Requirements](#requirements)
- [Features](#features)
- [Usage](#usage)
  - [Unit Test](#unit-test)
  - [Configuration](#configuration)
  - [Submit Job](#submit-a-job-to-google-cloud-dataflow)

## Requirements

- **Java Version**: 17 or higher
- **Build Tool**: Maven or Gradle
- **Dependencies:**
  - Apache Beam with Kafka integration SDK (`org.apache.beam:beam-sdks-java-io-kafka`)
  - Google Cloud Dataflow SDK (`org.apache.beam:beam-runners-google-cloud-dataflow-java`)
  - Google Cloud BigQuery SDK (`com.google.cloud:google-cloud-bigquery`)

## Features

- **Real-Time Data Processing**  
  Supports streaming data transformations using Apache Beam's `DoFn` functions.

- **BigQuery Integration**  
  Handles data schema transformations and writes to BigQuery tables.

- **Support for Change Data Capture (CDC)**  
  Processes CDC events (create, update, delete) with compatibility for Debezium MySQL, PostgreSQL, and MongoDB data streams.

- **Customizable Transformations**  
  Leverages [schemas](https://github.com/davidch93/etl/tree/main/etl-core#schema) and helper utilities to adapt data transformations to different use cases.  

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
  "config_bucket": "<your-config-bucket-bucket>",
  "kafka_config": {
    "bootstrap_servers": "<your-kafka-bootstrap-servers>",
    "group_id": "<your-group-id>",
    "default_offset": "earliest/latest"
  },
  "bigquery_config": {
    "project_id": "<your-project-id>",
    "region": "<your-region-id>",
    "dataset_id": "<your-prefix-dataset>",
    "create_disposition": "CREATE_IF_NEEDED/CREATE_NEVER",
    "partition_expiry_millis": 172800000,
    "temporary_gcs_bucket": "<your-temp-bucket-staging>"
  },
  "table_whitelist": [
    "<cluster>.<database>.<table1>",
    "<cluster>.<database>.<table2>"
  ]
}
```

### Submit a job to Google Cloud Dataflow

To submit a job, execute the following command.

```shell
./gradlew clean execute \
-Dorg.xerial.snappy.use.systemlib='true' \
-DmainClass='com.github.davidch93.etl.stream.EtlStreamApplication' \
-Dexec.args="
--streaming=true
--project=<your-project-id>
--region=<your-region>
--usePublicIps=false
--network=<your-network>
--subnetwork=<your-subnetwork>
--gcpTempLocation=<your-temp-location>
--tempLocation=<your-temp-location>
--serviceAccount=<your-service-account>
--runner=DataflowRunner
--jobName=<your-job-name>
--autoscalingAlgorithm=THROUGHPUT_BASED
--numWorkers=1
--maxNumWorkers=<your-max-workers>
--workerMachineType=<your-machine-type>
--workerDiskType=<your-disk-type>
--diskSizeGb=<your-disk-size>
--pipelineType=DATA_POOL_STREAM
--configPath=<your-filesystem>://<path>/config.json"
```
