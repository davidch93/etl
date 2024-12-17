package com.github.davidch93.etl.stream.transformers;

import com.github.davidch93.etl.core.schema.Table;
import com.github.davidch93.etl.core.utils.DateTimeUtils;
import com.github.davidch93.etl.core.utils.KafkaTopicResolver;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.github.davidch93.etl.core.constants.MetadataField.*;

/**
 * A Beam {@link DoFn} implementation to transform Kafka records into BigQuery {@link TableRow}'s
 * for Data Pool stream pipeline processing.
 * <p>
 * This class reads Kafka records containing change data capture (CDC) events, extracts the relevant information
 * based on the record's topic, transforms it into BigQuery TableRow format using the appropriate schema,
 * and emits the transformed TableRow for further processing in the pipeline.
 * </p>
 *
 * @author david.christianto
 */
public class DataPoolStream extends DoFn<KafkaRecord<String, String>, TableRow> {

    private static final Logger logger = LoggerFactory.getLogger(DataPoolStream.class);

    private final Map<String, Table> tablesByName;
    private final Counter errorRecords = Metrics.counter("DataPoolStream", "dataflow.pipeline.converter.error.count");

    /**
     * Constructs a new {@code DataPoolStream} instance with the specified schema by table name mapping.
     *
     * @param tablesByName a mapping of table names to their corresponding schemas.
     */
    private DataPoolStream(Map<String, Table> tablesByName) {
        this.tablesByName = tablesByName;
    }

    /**
     * Creates a new {@code DataPoolStream} instance with the provided schema by table name mapping.
     *
     * @param tablesByName a mapping of table names to their corresponding schemas.
     * @return a new {@code DataPoolStream} instance.
     */
    public static DataPoolStream transform(Map<String, Table> tablesByName) {
        return new DataPoolStream(tablesByName);
    }

    /**
     * Processes a single element {@code KafkaRecord} and transforms it into a BigQuery {@code TableRow}.
     *
     * @param kafkaRecord the {@code KafkaRecord} containing the key-value pair of the Kafka record.
     * @param out         the output receiver for emitting transformed TableRows.
     */
    @ProcessElement
    public void processElement(@Element KafkaRecord<String, String> kafkaRecord, OutputReceiver<TableRow> out) {
        try {
            KafkaTopicResolver resolver = KafkaTopicResolver.resolve(kafkaRecord.getTopic());
            String tableName = "%s_%s".formatted(resolver.getDatabaseName(), resolver.getTableName());
            Table table = tablesByName.get(tableName);

            TableRow row = transform(kafkaRecord.getKV(), table)
                .set(TABLE_NAME, table.getName())
                .set(SOURCE, table.getSource())
                .set(AUDIT_WRITE_TIME, formatCurrentTimestamp());

            out.output(row);
        } catch (Exception ex) {
            logger.error("[ETL-STREAM] Error parsing JSON to TableRow was found! Kafka: {}", kafkaRecord.getKV(), ex);
            errorRecords.inc();
        }
    }

    /**
     * Transforms a Kafka record into a BigQuery {@code TableRow} based on the table source type.
     *
     * @param record the key-value pair of the Kafka record.
     * @param table  the table schema definition corresponding to the table associated with the Kafka record.
     * @return the transformed {@code TableRow}.
     * @throws IllegalArgumentException if an unsupported source type is found.
     */
    private TableRow transform(KV<String, String> record, Table table) {
        return switch (table.getSource()) {
            case MYSQL -> DataPoolStreamMySqlTransformer.transform(record, table);
            case POSTGRESQL -> DataPoolStreamPostgreSqlTransformer.transform(record, table);
            case MONGODB -> DataPoolStreamMongoDbTransformer.transform(record, table);
            default -> throw new IllegalArgumentException("Unsupported source type for `" + table.getSource() + "`!");
        };
    }

    /**
     * Formats the current timestamp based on the fixed current time in milliseconds.
     * <p>
     * This method utilizes {@link org.joda.time.DateTimeUtils#setCurrentMillisFixed(long)}
     * to allow setting a fixed point in time for testing purposes. By relying on
     * {@link org.joda.time.DateTimeUtils#currentTimeMillis()}, the method ensures that
     * the timestamp reflects the mocked or fixed current time, making it ideal for
     * deterministic and reproducible tests.
     * </p>
     *
     * @return The formatted current timestamp as a {@link String}, using the specified {@code TIMESTAMP_FORMAT}.
     */
    private String formatCurrentTimestamp() {
        long epochMillis = org.joda.time.DateTimeUtils.currentTimeMillis();
        return DateTimeUtils.formatEpochMillis(epochMillis, TIMESTAMP_FORMAT);
    }
}
