package com.github.davidch93.etl.stream.helper;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.davidch93.etl.core.schema.Field.FieldType;
import com.github.davidch93.etl.core.utils.JsonUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RowTest {

    @Test
    void testConvert_withValidJson_thenExpectValidValue() {
        String jsonString = "{\"name\":\"John\",\"age\":30,\"is_married\":false,\"weight\":79.99,\"rules\":[]}";
        JsonNode jsonNode = JsonUtils.toJsonNode(jsonString);

        Object objectRow = Row.convert(jsonNode, FieldType.STRING);
        assertThat(objectRow).isEqualTo(jsonString);

        Object longObject = Row.convert(jsonNode.get("age"), FieldType.INTEGER);
        assertThat(longObject).isEqualTo(30L);

        Object booleanObject = Row.convert(jsonNode.get("is_married"), FieldType.BOOLEAN);
        assertThat(booleanObject).isEqualTo(false);

        Object doubleObject = Row.convert(jsonNode.get("weight"), FieldType.DOUBLE);
        assertThat(doubleObject).isEqualTo(79.99);

        Object arrayObject = Row.convert(jsonNode.get("rules"), FieldType.STRING);
        assertThat(arrayObject).isEqualTo("[]");

        Object stringObject = Row.convert(jsonNode.get("name"), FieldType.STRING);
        assertThat(stringObject).isEqualTo("John");
    }

    @Test
    void testConvert_withNullValue_thenExpectThrowsRuntimeException() {
        assertThatThrownBy(() -> Row.convert(null, FieldType.STRING))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("The Json value must not be null!");
    }

    @Test
    void testConvert_withUnsupportedFieldType_thenExpectThrowsRuntimeException() {
        String jsonString = "1708646400000";
        JsonNode jsonNode = JsonUtils.toJsonNode(jsonString);

        assertThatThrownBy(() -> Row.convert(jsonNode, FieldType.DECIMAL))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Unsupported field type was found: `DECIMAL`!");
    }

    @Test
    void testConvert_withNumericAsString_thenExpectStringValue() {
        String jsonString = "{\"age\":30,\"weight\":79.99}";
        JsonNode jsonNode = JsonUtils.toJsonNode(jsonString);

        Object longObject = Row.convert(jsonNode.get("age"), FieldType.STRING);
        assertThat(longObject).isEqualTo("30");

        Object doubleObject = Row.convert(jsonNode.get("weight"), FieldType.STRING);
        assertThat(doubleObject).isEqualTo("79.99");
    }

    @Test
    void testConvert_withNumericInString_thenExpectParsedValueToNumeric() {
        String jsonString = "{\"age\":\"30\",\"weight\":\"79.99\"}";
        JsonNode jsonNode = JsonUtils.toJsonNode(jsonString);

        Object longObject = Row.convert(jsonNode.get("age"), FieldType.INTEGER);
        assertThat(longObject).isEqualTo(30L);

        Object doubleObject = Row.convert(jsonNode.get("weight"), FieldType.DOUBLE);
        assertThat(doubleObject).isEqualTo(79.99);
    }

    @Test
    void testConvert_withNumericAsDifferentPrecision_thenExpectNumericWithNewPrecision() {
        String jsonString = "{\"age\":30,\"weight\":79.99}";
        JsonNode jsonNode = JsonUtils.toJsonNode(jsonString);

        Object doubleObject = Row.convert(jsonNode.get("age"), FieldType.DOUBLE);
        assertThat(doubleObject).isEqualTo(30.0);

        Object longObject = Row.convert(jsonNode.get("weight"), FieldType.INTEGER);
        assertThat(longObject).isEqualTo(79L);
    }
}
