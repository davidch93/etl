package com.github.davidch93.etl.core.schema;

import com.github.davidch93.etl.core.constants.Source;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.github.davidch93.etl.core.schema.Field.FieldType;
import static com.github.davidch93.etl.core.schema.FieldRule.Rule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaLoaderTest {

    @Test
    void testLoadTableSchema() {
        String schemaFilePath = "src/test/resources/schema/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);

        // Assert table-level properties
        assertThat(table).isNotNull();
        assertThat(table.getName()).isEqualTo("github_staging_orders");
        assertThat(table.getSource()).isEqualTo(Source.MYSQL);
        assertThat(table.getConstraintKeys()).isNotEmpty().containsExactly("id");
        assertThat(table.getClusteredColumns()).isNotEmpty().containsExactly("id");

        // Assert table partition
        TablePartition tablePartition = table.getTablePartition()
            .orElseThrow(() -> new RuntimeException("The table partition must not be null!"));
        assertThat(tablePartition.getSourceColumn()).isEqualTo("created_at");
        assertThat(tablePartition.getPartitionColumn()).isEqualTo("order_timestamp");
        assertThat(tablePartition.getPartitionType()).isEqualTo("DAY");
        assertThat(tablePartition.isPartitionFilterRequired()).isFalse();
        assertThat(tablePartition.getDescription())
            .isEqualTo("A partition column indicating the date of the order was created");

        // Assert TableSchema
        TableSchema schema = table.getSchema();
        assertThat(schema).isNotNull();
        assertThat(schema.getType()).isEqualTo("record");
        assertThat(schema.getName()).isEqualTo("orders");
        assertThat(schema.getDescription()).isEqualTo("Schema for the orders table");

        // Assert fields
        List<Field> fields = schema.getFields();
        assertThat(fields)
            .hasSize(5)
            .extracting("name")
            .containsExactly("id", "amount", "status", "is_expired", "created_at");

        // Field: id
        Field idField = fields.get(0);
        assertThat(idField.getName()).isEqualTo("id");
        assertThat(idField.getType()).isEqualTo(FieldType.INTEGER);
        assertThat(idField.isNullable()).isFalse();
        assertThat(idField.getDescription()).isEqualTo("Unique identifier for the order");
        assertThat(idField.getRules()).hasSize(1);
        assertThat(idField.getRules().get(0).getRule()).isEqualTo(Rule.IS_PRIMARY_KEY);
        assertThat(idField.getRules().get(0).getHint())
            .isEqualTo("The value of this column must be unique and not NULL!");

        // Field: amount
        Field amountField = fields.get(1);
        assertThat(amountField.getName()).isEqualTo("amount");
        assertThat(amountField.getType()).isEqualTo(FieldType.DOUBLE);
        assertThat(amountField.isNullable()).isTrue();
        assertThat(amountField.getDescription()).isEqualTo("Total amount of the order");
        assertThat(amountField.getRules()).hasSize(2);
        assertThat(amountField.getRules().get(0).getRule()).isEqualTo(Rule.IS_NON_NEGATIVE);
        assertThat(amountField.getRules().get(0).getHint()).isEqualTo("The value of this column must be positive!");
        assertThat(amountField.getRules().get(1).getRule()).isEqualTo(Rule.HAS_COMPLETENESS);
        assertThat(amountField.getRules().get(1).getAssertion()).isEqualTo(0.85);
        assertThat(amountField.getRules().get(1).getHint())
            .isEqualTo("NULL values of this column should be below 0.15!");

        // Field: status
        Field statusField = fields.get(2);
        assertThat(statusField.getName()).isEqualTo("status");
        assertThat(statusField.getType()).isEqualTo(FieldType.STRING);
        assertThat(statusField.isNullable()).isTrue();
        assertThat(statusField.getDescription()).isEqualTo("Status of the order");
        assertThat(statusField.getRules()).hasSize(1);
        assertThat(statusField.getRules().get(0).getRule()).isEqualTo(Rule.IS_CONTAINED_IN);
        assertThat(statusField.getRules().get(0).getValues()).containsExactly("PAID", "CANCELLED", "REMITTED");
        assertThat(statusField.getRules().get(0).getHint())
            .isEqualTo("The value of this column must contain PAID, CANCELLED, and REMITTED only");

        // Field: is_expired
        Field isExpiredField = fields.get(3);
        assertThat(isExpiredField.getName()).isEqualTo("is_expired");
        assertThat(isExpiredField.getType()).isEqualTo(FieldType.BOOLEAN);
        assertThat(isExpiredField.isNullable()).isTrue();
        assertThat(isExpiredField.getDescription()).isEqualTo("Whether the order expires or not");
        assertThat(isExpiredField.getRules()).isEmpty();

        // Field: created_at
        Field createdAtField = fields.get(4);
        assertThat(createdAtField.getName()).isEqualTo("created_at");
        assertThat(createdAtField.getType()).isEqualTo(FieldType.INTEGER);
        assertThat(createdAtField.isNullable()).isFalse();
        assertThat(createdAtField.getDescription()).isEqualTo("Date of the order");
        assertThat(createdAtField.getRules()).hasSize(1);
        assertThat(createdAtField.getRules().get(0).getRule()).isEqualTo(FieldRule.Rule.IS_COMPLETE);
        assertThat(createdAtField.getRules().get(0).getHint())
            .isEqualTo("The value of this column must not be NULL!");
    }

    @Test
    void testLoadTableSchema_withInvalidTableSchemaFile() {
        String invalidPath = "src/test/resources/schema/invalid-schema.json";

        assertThatThrownBy(() -> SchemaLoader.loadTableSchema(invalidPath))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to load schema from `" + invalidPath + "`!");
    }

    @Test
    void testLoadTableSchema_withMalformedJson() throws IOException {
        Path malformedPath = Paths.get("src/test/resources/schema/malformed-schema.json");
        Files.write(malformedPath, "{table_name: github_staging_orders, schema:}".getBytes(StandardCharsets.UTF_8));

        try {
            assertThatThrownBy(() -> SchemaLoader.loadTableSchema(malformedPath.toString()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to map JSON to object from input stream!");
        } finally {
            Files.deleteIfExists(malformedPath);
        }
    }
}
