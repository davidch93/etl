package com.github.davidch93.etl.sentry.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.github.davidch93.etl.core.config.BigQueryConfig

/**
 * The main configuration class to define settings for data quality validation
 * and processing in an ETL pipeline.
 *
 * @param groupName             The group name of the job.
 * @param maxThreads            The maximum number of threads to run the job.
 * @param configBucket          The GCS bucket where configuration files are stored.
 * @param deequRepositoryConfig The BigQuery configuration for the Deequ metrics repository.
 *                              This repository stores data quality metrics and validation results.
 * @param sourceBigQueryConfig  The BigQuery configuration for the source data.
 *                              This configuration is used to access the tables being validated or processed.
 * @param tableWhitelist        A list of table names that are included in the data quality validation process.
 *                              Only tables specified in this whitelist will be processed by the application.
 * @author david.christianto
 */
case class SentryConfiguration(
  @JsonProperty(value = "group_name", required = true) groupName: String,
  @JsonProperty(value = "max_threads", required = true) maxThreads: Int,
  @JsonProperty(value = "config_bucket", required = true) configBucket: String,
  @JsonProperty(value = "deequ_repository_config", required = true) deequRepositoryConfig: BigQueryConfig,
  @JsonProperty(value = "source_bigquery_config", required = true) sourceBigQueryConfig: BigQueryConfig,
  @JsonProperty(value = "table_whitelist", required = true) tableWhitelist: Array[String],
)
