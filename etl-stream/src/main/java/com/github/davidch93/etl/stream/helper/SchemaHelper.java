package com.github.davidch93.etl.stream.helper;

import com.github.davidch93.etl.core.schema.Table;
import com.github.davidch93.etl.core.utils.JsonUtils;
import com.google.cloud.storage.Storage;

/**
 * Utility class for managing schemas retrieved from Google Cloud Storage (GCS) for a given topic.
 * <p>
 * This class provides a method to read and deserialize table schema definitions GCS buckets.
 *
 * <p>
 * Example usage:
 * <pre>{@code
 * String bucketName = "your_bucket_name";
 * SchemaHelper schemaHelper = new SchemaHelper(bucketName);
 * Table table = schemaHelper.loadTableSchema("your_topic_name");
 * }</pre>
 * </p>
 *
 * @author david.christianto
 */
public class SchemaHelper {

    private final GcsHelper gcsHelper;
    private final String bucket;

    /**
     * Constructs a SchemaHelper instance with a default GcsHelper using the specified bucket.
     *
     * @param bucket the GCS bucket where schema files are stored.
     */
    public SchemaHelper(String bucket) {
        this.gcsHelper = new GcsHelper();
        this.bucket = bucket;
    }

    /**
     * Constructs a SchemaHelper instance with a custom Storage instance and the specified bucket.
     *
     * @param storage the custom Storage instance.
     * @param bucket  the GCS bucket where schema files are stored.
     */
    public SchemaHelper(Storage storage, String bucket) {
        this.gcsHelper = new GcsHelper(storage);
        this.bucket = bucket;
    }

    /**
     * Retrieves the schema definition for the given topic from the configured GCS bucket.
     *
     * @param topic The topic for which the schema is retrieved, in a format `clusterName.databaseName.tableName`.
     * @return the {@link Table} object representing the schema definition.
     */
    public Table loadTableSchema(String topic) {
        String[] tokens = topic.split("\\.");
        String schemaFilePath = String.format("schema/%s/%s/%s/schema.json", tokens[0], tokens[1], tokens[2]);
        String content = gcsHelper.readFile(bucket.replace("gs://", ""), schemaFilePath);

        return JsonUtils.readValue(content, Table.class);
    }
}
