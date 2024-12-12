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
 * Object value = Row.convert(jsonData, FieldType.INTEGER);
 * }</pre>
 * </p>
 *
 * <p>
 * Note: This utility class assumes that the provided JsonNode is not NULL.
 * <ul>
 * <li>If the JSON value is NULL, a {@code NullPointerException} will be thrown.</li>
 * <li>If the JSON value does not match the supported BigQuery type, a {@code IllegalArgumentException} will be thrown.</li>
 * </ul>
 * </p>
 *
 * @author david.christianto
 */
public final class Row {

    /**
     * Converts a JSON value into a specific data type based on the provided field type
     *
     * @param value     the JSON value to convert.
     * @param fieldType the field type to convert the JSON value to.
     * @return the JSON value converted to the specified data type.
     * @throws IllegalArgumentException if the field type is not supported.
     */
    public static Object convert(JsonNode value, FieldType fieldType) {
        Objects.requireNonNull(value, "The Json value must not be null!");

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
            throw new IllegalArgumentException(String.format("Unsupported field type was found: `%s`!", fieldType));
        }
    }
}
