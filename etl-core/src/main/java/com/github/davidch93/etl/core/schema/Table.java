package com.github.davidch93.etl.core.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Configuration class representing a table for ETL processing.
 * <p>
 * This class encapsulates details about a table's schema such as:
 * <ul>
 *   <li>The name of the table.</li>
 *   <li>The schema definition of the table, including its fields and validation rules.</li>
 *   <li>A list of keys representing primary or foreign key constraints.</li>
 *   <li>The configuration details for the table's partitions encapsulated in {@link TablePartition}.</li>
 *   <li>A list of clustered columns for optimizing queries.</li>
 * </ul>
 *
 * <p>
 * Instances of this class are serialized/deserialized using Jackson for JSON representation.
 * </p>
 *
 * <p><strong>Usage:</strong></p>
 * <ul>
 *     <li>This configuration class is designed to enable easy setup of table-level configurations
 *     for ETL processes, ensuring consistency and manageability across large-scale data pipelines.</li>
 *     <li>Default values are provided for certain fields. If no primary keys are specified,
 *     the default is a list containing "id".</li>
 * </ul>
 *
 * @author david.christianto
 */
public class Table implements Serializable {

    /**
     * Enumeration representing the supported types of source databases.
     * <p>
     * This enum is primarily used to categorize and identify the database
     * systems supported by the ETL (Extract, Transform, Load) process.
     * </p>
     *
     * <ul>
     *     <li>{@link #MYSQL} - Represents a MySQL database.</li>
     *     <li>{@link #POSTGRESQL} - Represents a PostgreSQL database.</li>
     *     <li>{@link #MONGODB} - Represents a MongoDB database.</li>
     *     <li>{@link #DYNAMODB} - Represents a DynamoDB database.</li>
     * </ul>
     */
    public enum Source {
        MYSQL,
        POSTGRESQL,
        MONGODB,
        DYNAMODB
    }

    @JsonProperty(value = "table_name", required = true)
    private String name;

    @JsonProperty(value = "source_type", required = true)
    private Source source;

    @JsonProperty(value = "schema", required = true)
    private TableSchema schema;

    @JsonProperty(value = "constraint_keys")
    private List<String> constraintKeys = List.of("id");

    @JsonProperty(value = "table_partition")
    private TablePartition tablePartition;

    @JsonProperty(value = "clustered_columns")
    private List<String> clusteredColumns;

    /**
     * Gets the table name.
     *
     * @return the table name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the source type.
     *
     * @return the source type
     */
    public Source getSource() {
        return source;
    }

    /**
     * Gets the schema definition of the table.
     *
     * @return The {@link TableSchema} instance.
     */
    public TableSchema getSchema() {
        return schema;
    }

    /**
     * Gets the list of constraint keys (e.g., primary or foreign keys).
     *
     * @return the list of constraint keys.
     */
    public List<String> getConstraintKeys() {
        return constraintKeys;
    }

    /**
     * Gets the partition configuration for the table.
     *
     * @return The {@link TablePartition} instance.
     */
    public TablePartition getTablePartition() {
        return tablePartition;
    }

    /**
     * Gets the list of clustered columns.
     *
     * @return the list of clustered columns.
     */
    public List<String> getClusteredColumns() {
        return clusteredColumns;
    }
}