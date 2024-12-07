package com.github.davidch93.etl.core.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Utility class for working with JSON using Jackson.
 * Provides methods for creating JSON nodes, parsing JSON content, and mapping JSON to Java objects.
 *
 * @author david.christianto
 */
public final class JsonUtils {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Creates an empty JSON object node.
     *
     * @return an empty {@link ObjectNode}.
     */
    public static ObjectNode createEmptyObjectNode() {
        return mapper.createObjectNode();
    }

    /**
     * Creates an empty JSON array node.
     *
     * @return an empty {@link ArrayNode}.
     */
    public static ArrayNode createEmptyArrayNode() {
        return mapper.createArrayNode();
    }

    /**
     * Converts an {@link InputStream} to a {@link JsonNode}.
     *
     * @param inputStream the input stream containing JSON content.
     * @return the parsed {@link JsonNode}.
     * @throws IllegalArgumentException if the input stream is null.
     * @throws RuntimeException         if the JSON content cannot be parsed.
     */
    public static JsonNode toJsonNode(InputStream inputStream) {
        Objects.requireNonNull(inputStream, "Input stream must not be null!");

        try {
            return mapper.readTree(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse JSON from input stream!", e);
        }
    }

    /**
     * Converts a JSON string to a {@link JsonNode}.
     *
     * @param content the JSON string.
     * @return the parsed {@link JsonNode}.
     * @throws IllegalArgumentException if the content is null or empty.
     * @throws RuntimeException         if the JSON content cannot be parsed.
     */
    public static JsonNode toJsonNode(String content) {
        if (StringUtils.isEmpty(content)) {
            throw new IllegalArgumentException("Content must not be null or empty!");
        }

        try {
            return mapper.readTree(content);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse JSON from string!", e);
        }
    }

    /**
     * Reads a JSON input stream into a Java object.
     *
     * @param inputStream the input stream containing JSON content.
     * @param classType   the target class type.
     * @param <T>         the type of the resulting object.
     * @return the parsed object.
     * @throws IllegalArgumentException if the input stream is null.
     * @throws RuntimeException         if the JSON content cannot be parsed.
     */
    public static <T> T readValue(InputStream inputStream, Class<T> classType) {
        Objects.requireNonNull(inputStream, "Input stream must not be null!");

        try {
            return mapper.readValue(inputStream, classType);
        } catch (IOException e) {
            throw new RuntimeException("Failed to map JSON to object from input stream!", e);
        }
    }

    /**
     * Reads a JSON string into a Java object.
     *
     * @param content   the JSON string.
     * @param classType the target class type.
     * @param <T>       the type of the resulting object.
     * @return the parsed object.
     * @throws IllegalArgumentException if the content is null or empty.
     * @throws RuntimeException         if the JSON content cannot be parsed.
     */
    public static <T> T readValue(String content, Class<T> classType) {
        if (StringUtils.isEmpty(content)) {
            throw new IllegalArgumentException("Content must not be null or empty!");
        }

        try {
            return mapper.readValue(content, classType);
        } catch (IOException e) {
            throw new RuntimeException("Failed to map JSON to object from string!", e);
        }
    }
}
