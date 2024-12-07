package com.github.davidch93.etl.core.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a field definition within a schema.
 * <p>
 * This class defines the name, type, and additional metadata for a field in a schema.
 * It supports validation rules and can be used to enforce data quality constraints.
 * </p>
 *
 * <p><strong>Field Types:</strong></p>
 * <ul>
 *   <li>BOOLEAN</li>
 *   <li>INTEGER</li>
 *   <li>DOUBLE</li>
 *   <li>DECIMAL</li>
 *   <li>STRING</li>
 * </ul>
 *
 * @author david.christianto
 */
public class Field implements Serializable {

    /**
     * Enumeration of field data types.
     */
    public enum FieldType {
        BOOLEAN,
        INTEGER,
        DOUBLE,
        DECIMAL,
        STRING
    }

    @JsonProperty(value = "name", required = true)
    private String name;

    @JsonProperty(value = "type", required = true)
    private FieldType type;

    @JsonProperty(value = "nullable")
    private boolean nullable = true;

    @JsonProperty(value = "doc")
    private String description = "";

    @JsonProperty(value = "rules")
    private List<FieldRule> rules;

    /**
     * Gets the name of the field.
     *
     * @return the field name, representing the attribute's identifier within the schema.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the data type of the field.
     *
     * @return the {@link FieldType} of the field, indicating its data type (e.g., STRING, INTEGER).
     */
    public FieldType getType() {
        return type;
    }

    /**
     * Checks whether the field allows null values.
     *
     * @return {@code true} if the field can contain null values, {@code false} otherwise.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Gets the description of the field.
     *
     * @return the field description, providing context or documentation about its purpose.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Gets the rules applied to the field.
     *
     * @return a list of {@link FieldRule} objects defining constraints or validation rules for the field.
     */
    public List<FieldRule> getRules() {
        return rules;
    }
}
