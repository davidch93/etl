package com.github.davidch93.etl.stream.transformers;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.davidch93.etl.core.schema.Field;
import com.github.davidch93.etl.core.schema.Table;
import com.github.davidch93.etl.core.utils.JsonUtils;
import com.github.davidch93.etl.stream.helper.Row;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.values.KV;

import static com.github.davidch93.etl.core.constants.MetadataField.*;

/**
 * Transformer class for processing MySQL change data capture (CDC) records from a streaming source.
 * Converts raw JSON payloads into BigQuery-compatible {@link TableRow} instances.
 *
 * @author david.christianto
 */
final class DataPoolStreamMySqlTransformer {

    /**
     * Transforms a key-value pair record into a BigQuery {@link TableRow} using the provided table schema.
     *
     * @param record the key-value record, where the value contains a CDC payload as JSON.
     * @param table  the table schema definition used to validate and transform the record.
     * @return a {@link TableRow} object containing the transformed data.
     */
    public static TableRow transform(KV<String, String> record, Table table) {
        JsonNode payload = JsonUtils.toJsonNode(record.getValue()).get(PAYLOAD);

        String operation = payload.get(PAYLOAD_OP).asText();
        boolean isDeleteOperation = operation.equalsIgnoreCase(PAYLOAD_OP_D);

        JsonNode data = isDeleteOperation ? payload.get(PAYLOAD_BEFORE) : payload.get(PAYLOAD_AFTER);
        JsonNode source = payload.get(PAYLOAD_SOURCE);

        return constructTableRow(data, table)
            .set(IS_DELETED, isDeleteOperation)
            .set(OP, operation)
            .set(TS_MS, source.get(PAYLOAD_SOURCE_TS_MS).asLong())
            .set(FILE, source.get(PAYLOAD_SOURCE_FILE).asText())
            .set(POS, source.get(PAYLOAD_SOURCE_POS).asLong());
    }

    /**
     * Constructs a {@link TableRow} instance by mapping fields from the provided JSON record
     * to the table schema. This method is optimized for high-frequency calls in Apache Beam pipelines.
     *
     * <p>Performance Considerations:</p>
     * <ul>
     *   <li><b>Direct Population:</b> Fields are directly mapped to the {@link TableRow} without
     *   creating intermediate collections, reducing memory allocations and garbage collection (GC) pressure.</li>
     *   <li><b>Loop-Based Processing:</b> An enhanced for-loop is used instead of streams to minimize
     *   overhead associated with lambda expressions and iterators, providing consistent performance
     *   for high-frequency calls.</li>
     * </ul>
     *
     * @param record the JSON record containing field values to map into the {@link TableRow}.
     *               It must not be null.
     * @param table  the {@link Table} instance that defines the schema used for mapping fields.
     *               It must not be null.
     * @return a populated {@link TableRow} object with values mapped from the input record.
     */
    private static TableRow constructTableRow(JsonNode record, Table table) {
        TableRow tableRow = new TableRow();
        for (Field field : table.getSchema().getFields()) {
            if (record.has(field.getName()) && !record.get(field.getName()).isNull()) {
                tableRow.set(field.getName(), Row.convert(record.get(field.getName()), field.getType()));
            }
        }

        return tableRow;
    }
}
