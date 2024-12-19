package com.github.davidch93.etl.stream.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.davidch93.etl.core.constants.Source;
import com.github.davidch93.etl.core.schema.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaHelperTest {

    private static SchemaHelper schemaHelper;

    @BeforeAll
    static void setup() throws JsonProcessingException {
        Storage localStorage = LocalStorageHelper.getOptions().getService();

        String bucket = "test-bucket";
        String schemaFilePath = "schema/mysql/github_staging_orders/schema.json";

        Table table = SchemaLoader.loadTableSchema("src/test/resources/" + schemaFilePath);
        byte[] jsonBytes = new ObjectMapper().writeValueAsBytes(table);

        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, schemaFilePath)).build();
        localStorage.create(blobInfo, jsonBytes);

        schemaHelper = new SchemaHelper(localStorage, bucket);
    }

    @Test
    void testLoadTableSchema_whenSchemaExists_thenExpectValidSchema() {
        String topic = "mysqlstaging.github_staging.orders";
        Table table = schemaHelper.loadTableSchema(topic);

        // Assert table-level properties
        assertThat(table).isNotNull();
        assertThat(table.getName()).isEqualTo("github_staging_orders");
        assertThat(table.getSource()).isEqualTo(Source.MYSQL);
        assertThat(table.getConstraintKeys()).isNotEmpty().containsExactly("id");
        assertThat(table.getClusteredColumns()).isNotEmpty().containsExactly("id");

        // Assert table partition
        TablePartition tablePartition = table.getTablePartition();
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
        assertThat(idField.getType()).isEqualTo(Field.FieldType.INTEGER);
        assertThat(idField.isNullable()).isFalse();
        assertThat(idField.getDescription()).isEqualTo("Unique identifier for the order");
        assertThat(idField.getRules()).hasSize(1);
        assertThat(idField.getRules().get(0).getRule()).isEqualTo(FieldRule.Rule.IS_PRIMARY_KEY);
        assertThat(idField.getRules().get(0).getHint())
            .isEqualTo("The value of this column must be unique and not NULL!");

        // Field: amount
        Field amountField = fields.get(1);
        assertThat(amountField.getName()).isEqualTo("amount");
        assertThat(amountField.getType()).isEqualTo(Field.FieldType.DOUBLE);
        assertThat(amountField.isNullable()).isTrue();
        assertThat(amountField.getDescription()).isEqualTo("Total amount of the order");
        assertThat(amountField.getRules()).hasSize(2);
        assertThat(amountField.getRules().get(0).getRule()).isEqualTo(FieldRule.Rule.IS_NON_NEGATIVE);
        assertThat(amountField.getRules().get(0).getHint()).isEqualTo("The value of this column must be positive!");
        assertThat(amountField.getRules().get(1).getRule()).isEqualTo(FieldRule.Rule.HAS_COMPLETENESS);
        assertThat(amountField.getRules().get(1).getAssertion()).isEqualTo(0.85);
        assertThat(amountField.getRules().get(1).getHint())
            .isEqualTo("NULL values of this column should be below 0.15!");

        // Field: status
        Field statusField = fields.get(2);
        assertThat(statusField.getName()).isEqualTo("status");
        assertThat(statusField.getType()).isEqualTo(Field.FieldType.STRING);
        assertThat(statusField.isNullable()).isTrue();
        assertThat(statusField.getDescription()).isEqualTo("Status of the order");
        assertThat(statusField.getRules()).hasSize(1);
        assertThat(statusField.getRules().get(0).getRule()).isEqualTo(FieldRule.Rule.IS_CONTAINED_IN);
        assertThat(statusField.getRules().get(0).getValues()).containsExactly("PAID", "CANCELLED", "REMITTED");
        assertThat(statusField.getRules().get(0).getHint())
            .isEqualTo("The value of this column must contain PAID, CANCELLED, and REMITTED only");

        // Field: is_expired
        Field isExpiredField = fields.get(3);
        assertThat(isExpiredField.getName()).isEqualTo("is_expired");
        assertThat(isExpiredField.getType()).isEqualTo(Field.FieldType.BOOLEAN);
        assertThat(isExpiredField.isNullable()).isTrue();
        assertThat(isExpiredField.getDescription()).isEqualTo("Whether the order expires or not");
        assertThat(isExpiredField.getRules()).isNull();

        // Field: created_at
        Field createdAtField = fields.get(4);
        assertThat(createdAtField.getName()).isEqualTo("created_at");
        assertThat(createdAtField.getType()).isEqualTo(Field.FieldType.INTEGER);
        assertThat(createdAtField.isNullable()).isFalse();
        assertThat(createdAtField.getDescription()).isEqualTo("Date of the order");
        assertThat(createdAtField.getRules()).hasSize(1);
        assertThat(createdAtField.getRules().get(0).getRule()).isEqualTo(FieldRule.Rule.IS_COMPLETE);
        assertThat(createdAtField.getRules().get(0).getHint())
            .isEqualTo("The value of this column must not be NULL!");
    }

    @Test
    void testLoadTableSchema_whenSchemaNotExist_thenExpectThrowsException() {
        String topic = "mysqlstaging.github_staging.users";

        assertThatThrownBy(() -> schemaHelper.loadTableSchema(topic))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("File `gs://test-bucket/schema/mysql/github_staging_users/schema.json` is not found!");
    }
}
