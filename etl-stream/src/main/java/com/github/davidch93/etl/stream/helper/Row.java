package com.github.davidch93.etl.stream.helper;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.davidch93.etl.core.schema.Field.FieldType;

import java.util.Objects;

/**
 * A utility class for converting a single row of data.
 * This class provides methods to convert JSON data into various BigQuery-compatible data types based on the provided schema.
 * <p>
 * Example usage:
 * <pre>{@code
 * JsonNode jsonData = ...; // JSON data representing a single row
 * Schema schema = ...; // The schema defining the structure of the data
 * Object value = Row.convert(jsonData).as(FieldType.INTEGER);
 * }</pre>
 * </p>
 *
 * <p>
 * Note: This utility class assumes that the provided JsonNode is not NULL.
 * <ul>
 * <li>If the JSON value is NULL, a {@code NullPointerException} will be thrown.</li>
 * <li>If the JSON value does not match the supported BigQuery type, a {@code RuntimeException} will be thrown.</li>
 * </ul>
 * </p>
 *
 * @author david.christianto
 */
public class Row {

    private final JsonNode value;

    private Row(JsonNode value) {
        this.value = value;
    }

    /**
     * Converts a JSON value into a Row instance.
     *
     * @param value The JSON value to convert.
     * @return The Row instance encapsulating the JSON value.
     * @throws NullPointerException if the provided JSON value is null.
     */
    public static Row convert(JsonNode value) {
        Objects.requireNonNull(value, "The Json value must not be null!");
        return new Row(value);
    }

    /**
     * Retrieves the JSON value of the row as a specific data type based on the provided field type.
     *
     * @param fieldType The field type to convert the JSON value to.
     * @return The JSON value converted to the specified data type.
     * @throws RuntimeException if the field type is not supported.
     */
    public Object as(FieldType fieldType) {
        if (value.isBoolean()) {
            return value.booleanValue();
        } else if (value.isObject() || value.isArray()) {
            return value.toString();
        } else if (fieldType == FieldType.INTEGER) {
            return value.asLong();
        } else if (fieldType == FieldType.DOUBLE) {
            return value.asDouble();
        } else if (fieldType == FieldType.STRING) {
            return value.asText();
        } else {
            throw new RuntimeException(String.format("Unsupported field type was found: `%s`!", fieldType));
        }
    }
}
