package com.github.davidch93.etl.stream.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.davidch93.etl.core.config.BigQueryConfig;
import com.github.davidch93.etl.core.config.KafkaConfig;

import java.io.Serializable;
import java.util.List;

/**
 * The main configuration class to store information about data pool stream pipeline.
 *
 * <p>This class represents the overall configuration required for setting up and running a real-time pipeline.
 * It contains details about the Kafka, BigQuery, and table whitelist configuration.
 *
 * @author david.christianto
 */
public class StreamConfiguration implements Serializable {

    @JsonProperty(value = "config_bucket", required = true)
    private String configBucket;

    @JsonProperty(value = "kafka_config", required = true)
    private KafkaConfig kafkaConfig;

    @JsonProperty(value = "bigquery_config", required = true)
    private BigQueryConfig bigQueryConfig;

    @JsonProperty(value = "table_whitelist", required = true)
    private List<String> tableWhitelist;

    /**
     * Get the config bucket.
     *
     * @return The config bucket.
     */
    public String getConfigBucket() {
        return configBucket;
    }

    /**
     * Get the Kafka config.
     *
     * @return The Kafka config.
     */
    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    /**
     * Get the BigQuery config.
     *
     * @return The BigQuery config.
     */
    public BigQueryConfig getBigQueryConfig() {
        return bigQueryConfig;
    }

    /**
     * Get the table whitelist.
     *
     * @return The table whitelist.
     */
    public List<String> getTableWhitelist() {
        return tableWhitelist;
    }
}
