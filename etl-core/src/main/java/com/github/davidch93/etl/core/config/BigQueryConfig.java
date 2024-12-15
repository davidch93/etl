package com.github.davidch93.etl.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.davidch93.etl.core.constants.Dataset;
import com.github.davidch93.etl.core.constants.Source;

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
 * <p><strong>Note:</strong>
 * This class is designed for use with Jackson for JSON serialization and deserialization.</p>
 *
 * @author david.christianto
 */
public class BigQueryConfig implements Serializable {

    @JsonProperty(value = "project_id", required = true)
    private String projectId;

    @JsonProperty(value = "region", required = true)
    private String region;

    @JsonProperty(value = "dataset_id")
    private String datasetId;

    @JsonProperty(value = "create_disposition")
    private String createDisposition;

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
     * Sets the Google Cloud project ID associated with the BigQuery configuration.
     *
     * @param projectId The project ID.
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId;
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
     * Sets the Google Cloud region where BigQuery resources are located.
     *
     * @param region The region.
     */
    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * Gets the dataset ID of BigQuery.
     * <p><strong>Note:</strong> It also could be a prefix of the dataset ID.</p>
     *
     * @return The dataset ID of BigQuery.
     */
    public String getDatasetId() {
        return datasetId;
    }

    /**
     * Sets the dataset ID of BigQuery.
     * <p><strong>Note:</strong> It also could be a prefix of the dataset ID.</p>
     *
     * @param datasetId The dataset ID of BigQuery.
     */
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
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
     * Sets the table creation disposition for BigQuery operations.
     *
     * @param createDisposition The table creation disposition.
     */
    public void setCreateDisposition(String createDisposition) {
        this.createDisposition = createDisposition;
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
     * Sets the partition expiration time in milliseconds.
     *
     * @param partitionExpiryMillis The partition expiration time in milliseconds.
     */
    public void setPartitionExpiryMillis(Long partitionExpiryMillis) {
        this.partitionExpiryMillis = partitionExpiryMillis;
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

    /**
     * Sets  the name of the temporary Google Cloud Storage bucket used for intermediate data storage.
     *
     * @param temporaryGcsBucket The temporary GCS bucket name, or {@code null} if not specified.
     */
    public void setTemporaryGcsBucket(String temporaryGcsBucket) {
        this.temporaryGcsBucket = temporaryGcsBucket;
    }

    /**
     * Constructs the dataset ID for a specific dataset and source system.
     *
     * @param dataset the {@link Dataset} indicating the type of dataset (e.g., STREAM, DAILY, REAL_TIME).
     * @param source  the {@link Source} system of the dataset (e.g., MYSQL, POSTGRESQL, MONGODB).
     * @return the constructed dataset ID as a {@code String}.
     */
    public String getDatasetId(Dataset dataset, Source source) {
        return String.format("%s_%s_%s", datasetId, dataset.toString().toLowerCase(), source.toString().toLowerCase());
    }

    /**
     * Constructs the fully qualified table name for a specific dataset, source system, and table name.
     *
     * @param dataset the {@link Dataset} indicating the type of dataset (e.g., STREAM, DAILY, REAL_TIME).
     * @param source  the {@link Source} system of the dataset (e.g., MYSQL, POSTGRESQL, MONGODB).
     * @return the fully qualified table name as a {@code String}, in the format `projectId.datasetId.tableId`.
     */
    public String getFullyQualifiedTableName(Dataset dataset, Source source, String tableName) {
        return String.format("%s.%s.%s", projectId, getDatasetId(dataset, source), tableName);
    }
}
