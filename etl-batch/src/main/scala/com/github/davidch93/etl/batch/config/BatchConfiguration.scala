package com.github.davidch93.etl.batch.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.github.davidch93.etl.core.config.{BigQueryConfig, KafkaConfig}

/**
 * The main configuration class to define settings for batch processing in the ETL pipeline.
 *
 * This class is mapped directly from a JSON configuration file and contains all
 * necessary settings for managing data lake, data pool, Kafka, BigQuery, and other
 * resources used in the batch ETL process.
 *
 * @param groupName       The group name of the job; used to identify and group related batch jobs.
 * @param maxThreads      The maximum number of threads to use for parallel processing.
 * @param dataLakeBucket  The GCS bucket used to store raw or processed data in the data lake.
 * @param dataPoolBucket  The GCS bucket used for intermediate data storage (data pool).
 * @param configBucket    The GCS bucket containing configuration files and metadata.
 * @param kafkaConfig     The Kafka configuration for consuming messages.
 * @param bigQueryConfig  The BigQuery configuration for managing data ingestion and queries.
 * @param tableWhitelist  A list of table names allowed to be processed in the batch job.
 * @author david.christianto
 */
case class BatchConfiguration(
  @JsonProperty(value = "group_name", required = true) groupName: String,
  @JsonProperty(value = "max_threads", required = true) maxThreads: Int,
  @JsonProperty(value = "data_lake_bucket", required = true) dataLakeBucket: String,
  @JsonProperty(value = "data_pool_bucket", required = true) dataPoolBucket: String,
  @JsonProperty(value = "config_bucket", required = true) configBucket: String,
  @JsonProperty(value = "kafka_config", required = true) kafkaConfig: KafkaConfig,
  @JsonProperty(value = "bigquery_config", required = true) bigQueryConfig: BigQueryConfig,
  @JsonProperty(value = "table_whitelist", required = true) tableWhitelist: Array[String],
)
