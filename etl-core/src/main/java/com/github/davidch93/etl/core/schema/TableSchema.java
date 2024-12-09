package com.github.davidch93.etl.core.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a schema definition for data processing pipelines.
 * <p>
 * This class encapsulates details about a schema, including its type, name,
 * description, and the list of fields it contains. The schema is designed to
 * facilitate validation, transformation, and ingestion of data within ETL workflows.
 * </p>
 *
 * <p><strong>Usage:</strong> Instances of this class are typically serialized/deserialized
 * using Jackson for JSON representation in ETL configurations.</p>
 *
 * <p><strong>Example:</strong> A schema definition for a table or dataset in an ETL process.</p>
 *
 * @author david.christianto
 */
public class TableSchema implements Serializable {

    @JsonProperty(value = "type", required = true)
    private String type;

    @JsonProperty(value = "name", required = true)
    private String name;

    @JsonProperty(value = "doc")
    private String description = "";

    @JsonProperty(value = "fields", required = true)
    private List<Field> fields;

    /**
     * Gets the type of the schema.
     *
     * @return the schema type, typically representing the overall data structure (e.g., record, struct).
     */
    public String getType() {
        return type;
    }

    /**
     * Gets the name of the schema.
     *
     * @return the schema name, representing a logical identifier for the data structure.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the description of the schema.
     *
     * @return the schema description, providing context or documentation about its purpose.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Gets the fields defined in the schema.
     *
     * @return a list of {@link Field} objects representing the attributes or elements within the schema.
     */
    public List<Field> getFields() {
        return fields;
    }
}
