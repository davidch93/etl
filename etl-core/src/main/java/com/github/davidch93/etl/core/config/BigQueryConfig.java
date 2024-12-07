package com.github.davidch93.etl.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * A configuration class to encapsulate BigQuery-related properties for ETL operations.
 *
 * <p>
 * This class provides a structured representation of essential configurations required for interacting
 * with Google BigQuery. It includes details such as the project ID, region, dataset prefix, and other
 * important properties like table creation disposition and partition expiration.
 * </p>
 *
 * <p>
 * Instances of this class are typically loaded from a configuration file and passed to components
 * that perform operations involving BigQuery, such as creating tables, managing partitions, and
 * writing data to BigQuery datasets.
 * </p>
 *
 * <p><strong>Note:</strong> This class is designed for use with Jackson for JSON serialization and deserialization.</p>
 *
 * @author david.christianto
 */
public class BigQueryConfig implements Serializable {

    @JsonProperty(value = "project_id", required = true)
    private String projectId;

    @JsonProperty(value = "region", required = true)
    private String region;

    @JsonProperty(value = "create_disposition", required = true)
    private String createDisposition;

    @JsonProperty(value = "prefix_dataset_id", required = true)
    private String prefixDatasetId;

    @JsonProperty(value = "partition_expiry_millis")
    private Long partitionExpiryMillis;

    @JsonProperty(value = "temporary_gcs_bucket")
    private String temporaryGcsBucket;

    /**
     * Gets the Google Cloud project ID associated with the BigQuery configuration.
     *
     * @return The project ID.
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * Gets the Google Cloud region where BigQuery resources are located.
     *
     * @return The region.
     */
    public String getRegion() {
        return region;
    }

    /**
     * Gets the table creation disposition for BigQuery operations.
     * <p>
     * This property defines how tables are created during write operations, such as
     * whether to create a table if it does not exist or to throw an error if the table is missing.
     * </p>
     *
     * @return The table creation disposition.
     */
    public String getCreateDisposition() {
        return createDisposition;
    }

    /**
     * Gets the prefix used for dataset IDs in BigQuery.
     *
     * @return The prefix for dataset IDs.
     */
    public String getPrefixDatasetId() {
        return prefixDatasetId;
    }

    /**
     * Gets the partition expiration time in milliseconds.
     * <p>
     * This property specifies how long partitions in BigQuery tables remain active
     * before they are automatically deleted.
     * </p>
     *
     * @return The partition expiration time in milliseconds.
     */
    public Long getPartitionExpiryMillis() {
        return partitionExpiryMillis;
    }

    /**
     * Gets the name of the temporary Google Cloud Storage bucket used for intermediate data storage.
     * <p>
     * This property is optional and is typically used during BigQuery load jobs or write operations
     * that require temporary storage.
     * </p>
     *
     * @return The temporary GCS bucket name, or {@code null} if not specified.
     */
    public String getTemporaryGcsBucket() {
        return temporaryGcsBucket;
    }
}
