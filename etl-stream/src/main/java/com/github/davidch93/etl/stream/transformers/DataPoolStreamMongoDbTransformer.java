package com.github.davidch93.etl.stream.transformers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.davidch93.etl.core.schema.Field;
import com.github.davidch93.etl.core.schema.Table;
import com.github.davidch93.etl.core.utils.JsonUtils;
import com.github.davidch93.etl.stream.helpers.Row;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.values.KV;

import static com.github.davidch93.etl.core.constants.MetadataField.*;

/**
 * Transformer class for processing MongoDB change data capture (CDC) records from a streaming source.
 * Converts raw JSON payloads into BigQuery-compatible {@link TableRow} instances.
 *
 * @author david.christianto
 */
final class DataPoolStreamMongoDbTransformer {

    /**
     * Transforms a key-value pair record into a BigQuery {@link TableRow} using the provided table schema.
     *
     * @param record the key-value record, where the value contains a CDC payload as JSON.
     * @param table  the table schema definition used to validate and transform the record.
     * @return a {@link TableRow} object containing the transformed data.
     */
    public static TableRow transform(KV<String, String> record, Table table) {
        JsonNode id = JsonUtils.toJsonNode(record.getKey()).get(PAYLOAD).get(PAYLOAD_ID);
        JsonNode payload = JsonUtils.toJsonNode(record.getValue()).get(PAYLOAD);

        String operation = payload.get(PAYLOAD_OP).asText();
        JsonNode data = constructRecord(operation, id, payload);
        JsonNode source = payload.get(PAYLOAD_SOURCE);

        return constructTableRow(data, table)
            .set(IS_DELETED, operation.equalsIgnoreCase(PAYLOAD_OP_D))
            .set(OP, operation)
            .set(TS_MS, source.get(PAYLOAD_SOURCE_TS_MS).asLong())
            .set(ORD, source.get(PAYLOAD_SOURCE_ORD).asLong());
    }

    /**
     * Constructs the JSON record data based on the operation type and payload.
     *
     * <p>This method determines the JSON record data based on the operation type (e.g., insert, update, delete)
     * and extracts the relevant information from the payload accordingly.
     *
     * @param operation the operation type (e.g., "r" for read, "c" for create, "u" for update, and "d" for delete).
     * @param id        the JSON payload containing the id.
     * @param payload   the JSON payload containing the operation data.
     * @return the extracted JSON record data.
     */
    private static JsonNode constructRecord(String operation, JsonNode id, JsonNode payload) {
        if (operation.equalsIgnoreCase(PAYLOAD_OP_R) || operation.equalsIgnoreCase(PAYLOAD_OP_C)) {
            return JsonUtils.toJsonNode(payload.get(PAYLOAD_AFTER).asText());
        } else if (operation.equalsIgnoreCase(PAYLOAD_OP_U)) {
            return extractPatchData(payload.get(PAYLOAD_PATCH).asText()).put("_id", id.asText());
        } else {
            return JsonUtils.createEmptyObjectNode().put("_id", id.asText());
        }
    }

    /**
     * Extracts patch data from MongoDB payload in the following order.
     * <ol>
     * <li>Handle {@code $set} (MongoDB 3 & 4)</li>
     * <li>Handle {@code diff} with {@code u} and {@code i} (Mongo 5 or above)</li>
     * <li>Handle {@code diff} with {@code u} only (Mongo 5 or above)</li>
     * <li>Handle {@code diff} with {@code i} only (Mongo 5 or above)</li>
     * <li>Create an empty JSON with {@code _id} information only.</li>
     * </ol>
     *
     * @param patchString the patch string extracted from the MongoDB payload.
     * @return the extracted JSON patch data.
     */
    private static ObjectNode extractPatchData(String patchString) {
        JsonNode patch = JsonUtils.toJsonNode(patchString);
        if (patch.has(PAYLOAD_SET)) {
            return (ObjectNode) patch.get(PAYLOAD_SET);
        } else if (patch.has(PAYLOAD_DIFF) && patch.get(PAYLOAD_DIFF).has(PAYLOAD_DIFF_U) && patch.get(PAYLOAD_DIFF).has(PAYLOAD_DIFF_I)) {
            ObjectNode newPayload = JsonUtils.createEmptyObjectNode();
            newPayload.setAll((ObjectNode) patch.get(PAYLOAD_DIFF).get(PAYLOAD_DIFF_U));
            newPayload.setAll((ObjectNode) patch.get(PAYLOAD_DIFF).get(PAYLOAD_DIFF_I));
            return newPayload;
        } else if (patch.has(PAYLOAD_DIFF) && patch.get(PAYLOAD_DIFF).has(PAYLOAD_DIFF_U)) {
            return (ObjectNode) patch.get(PAYLOAD_DIFF).get(PAYLOAD_DIFF_U);
        } else if (patch.has(PAYLOAD_DIFF) && patch.get(PAYLOAD_DIFF).has(PAYLOAD_DIFF_I)) {
            return (ObjectNode) patch.get(PAYLOAD_DIFF).get(PAYLOAD_DIFF_I);
        } else {
            return (ObjectNode) patch;
        }
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
                JsonNode formattedJson = formatJson(record.get(field.getName()));
                tableRow.set(field.getName(), Row.convert(formattedJson, field.getType()));
            }
        }

        return tableRow;
    }

    /**
     * Formats the JSON payload for MongoDB.
     *
     * @param payload the JSON payload to format.
     * @return the formatted JSON payload.
     */
    private static JsonNode formatJson(JsonNode payload) {
        if (payload.has("$date")) {
            return payload.get("$date");
        } else if (payload.has("$numberInt")) {
            return payload.get("$numberInt");
        } else if (payload.has("$numberLong")) {
            return payload.get("$numberLong");
        } else if (payload.has("$numberDecimal")) {
            return payload.get("$numberDecimal");
        } else {
            return payload;
        }
    }
}
