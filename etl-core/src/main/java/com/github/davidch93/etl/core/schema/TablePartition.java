package com.github.davidch93.etl.core.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents the partition configuration for a table in ETL operations.
 * <p>
 * This class provides details about how a table is partitioned, including:
 * <ul>
 *   <li>The source column used in data processing.</li>
 *   <li>The column used for partitioning.</li>
 *   <li>The type of partitioning (e.g., time-based, range-based).</li>
 *   <li>Whether partition filtering is required during queries or operations.</li>
 * </ul>
 * <p>
 * Instances of this class are typically serialized/deserialized using Jackson for JSON representation.
 * </p>
 *
 * <p><strong>Usage:</strong> This class is designed to be part of a larger configuration framework for ETL pipelines,
 * enabling easy setup and maintenance of partitioned tables in data lakes or warehouses.</p>
 *
 * @author david.christianto
 */
public class TablePartition implements Serializable {

    @JsonProperty(value = "source_column", required = true)
    private String sourceColumn;

    @JsonProperty(value = "partition_column", required = true)
    private String partitionColumn;

    @JsonProperty(value = "partition_type")
    private String partitionType = "DAY";

    @JsonProperty(value = "partition_filter_required")
    private boolean partitionFilterRequired;

    /**
     * Gets the source column name.
     *
     * @return the source column name.
     */
    public String getSourceColumn() {
        return sourceColumn;
    }

    /**
     * Gets the partition column name.
     *
     * @return the partition column name.
     */
    public String getPartitionColumn() {
        return partitionColumn;
    }

    /**
     * Gets the type of partitioning for the table.
     *
     * @return The partition type (e.g., "TIME", "RANGE").
     */
    public String getPartitionType() {
        return partitionType;
    }

    /**
     * Determines if filtering by partition is required during operations.
     *
     * @return {@code true} if partition filtering is required; {@code false} otherwise.
     */
    public boolean isPartitionFilterRequired() {
        return partitionFilterRequired;
    }
}
