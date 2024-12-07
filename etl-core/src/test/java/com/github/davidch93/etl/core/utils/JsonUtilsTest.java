package com.github.davidch93.etl.core.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonUtilsTest {

    @Test
    void testCreateEmptyObjectNode() {
        ObjectNode objectNode = JsonUtils.createEmptyObjectNode();
        assertThat(objectNode).isNotNull().isEmpty();
    }

    @Test
    void testCreateEmptyArrayNode() {
        ArrayNode arrayNode = JsonUtils.createEmptyArrayNode();
        assertThat(arrayNode).isNotNull().isEmpty();
    }

    @Test
    void testToJsonNode_fromInputStream() {
        String json = "{\"key\":\"value\"}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        JsonNode jsonNode = JsonUtils.toJsonNode(inputStream);

        assertThat(jsonNode).isNotNull();
        assertThat(jsonNode.get("key").asText()).isEqualTo("value");
    }

    @Test
    void testToJsonNode_fromNullInputStream_expectThrowsException() {
        assertThatThrownBy(() -> JsonUtils.toJsonNode((InputStream) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("Input stream must not be null!");
    }

    @Test
    void testToJsonNode_withInvalidInputStream_expectThrowsException() {
        String invalidJson = "not-a-json";
        InputStream inputStream = new ByteArrayInputStream(invalidJson.getBytes());

        assertThatThrownBy(() -> JsonUtils.toJsonNode(inputStream))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to parse JSON from input stream!");
    }

    @Test
    void testToJsonNode_fromString() {
        String json = "{\"key\":\"value\"}";
        JsonNode jsonNode = JsonUtils.toJsonNode(json);

        assertThat(jsonNode).isNotNull();
        assertThat(jsonNode.get("key").asText()).isEqualTo("value");
    }

    @Test
    void testToJsonNode_fromEmptyString_expectThrowsException() {
        assertThatThrownBy(() -> JsonUtils.toJsonNode(""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Content must not be null or empty!");
    }

    @Test
    void testToJsonNode_fromInvalidString_expectThrowsException() {
        assertThatThrownBy(() -> JsonUtils.toJsonNode("not-a-json"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to parse JSON from string!");
    }

    @Test
    void testReadValue_fromInputStream() {
        String json = "{\"name\":\"John\", \"age\":30}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Person person = JsonUtils.readValue(inputStream, Person.class);

        assertThat(person).isNotNull();
        assertThat(person.getName()).isEqualTo("John");
        assertThat(person.getAge()).isEqualTo(30);
    }

    @Test
    void testReadValue_fromNullInputStream_expectThrowsException() {
        assertThatThrownBy(() -> JsonUtils.readValue((InputStream) null, Person.class))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("Input stream must not be null!");
    }

    @Test
    void testReadValue_withInvalidInputStreamForClassType_expectThrowsException() {
        String json = "{\"invalidField\":\"value\"}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());

        assertThatThrownBy(() -> JsonUtils.readValue(inputStream, Person.class))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to map JSON to object from input stream!");
    }

    @Test
    void testReadValueFromString() {
        String json = "{\"name\":\"John\", \"age\":30}";
        Person person = JsonUtils.readValue(json, Person.class);

        assertThat(person).isNotNull();
        assertThat(person.getName()).isEqualTo("John");
        assertThat(person.getAge()).isEqualTo(30);
    }

    @Test
    void testReadValue_fromEmptyString_expectThrowsException() {
        assertThatThrownBy(() -> JsonUtils.readValue("", Person.class))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Content must not be null or empty!");
    }

    @Test
    void testReadValue_fromInvalidString_expectThrowsException() {
        assertThatThrownBy(() -> JsonUtils.readValue("not-a-json", Person.class))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to map JSON to object from string!");
    }

    @Test
    void testReadValue_withInvalidJsonForClassType_expectThrowsException() {
        String json = "{\"invalidField\":\"value\"}";
        assertThatThrownBy(() -> JsonUtils.readValue(json, Person.class))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to map JSON to object from string!");
    }

    static class Person {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
