package com.github.davidch93.etl.stream.transformers;

import com.github.davidch93.etl.core.config.BigQueryConfig;
import com.github.davidch93.etl.core.schema.Table;
import com.github.davidch93.etl.core.utils.DateTimeUtils;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.github.davidch93.etl.core.constants.MetadataField.*;

/**
 * A Beam {@link DoFn} implementation to partition {@code TableRow} records based on specified criteria.
 * <p>
 * This class partitions {@code TableRow} records based on the configured partition column and the source column.
 * It checks if the source of the data is older than a specified expiry time and discards records accordingly.
 * The partitioned records are emitted for further processing in the pipeline.
 * </p>
 *
 * @author david.christianto
 */
public class Partitioner extends DoFn<TableRow, TableRow> {

    private static final Logger logger = LoggerFactory.getLogger(Partitioner.class);

    private final Map<String, Table> tablesByName;
    private final BigQueryConfig bigQueryConfig;
    private final Counter errorRecords = Metrics.counter("Partitioner", "dataflow.pipeline.partitioning.error.count");

    /**
     * Constructs a new {@code Partitioner} instance with the specified schema by table name mapping
     * and {@link BigQueryConfig BigQuery configuration}.
     *
     * @param tablesByName   a mapping of table names to their corresponding schemas.
     * @param bigQueryConfig the BigQuery configuration.
     */
    private Partitioner(Map<String, Table> tablesByName, BigQueryConfig bigQueryConfig) {
        this.tablesByName = tablesByName;
        this.bigQueryConfig = bigQueryConfig;
    }

    /**
     * Creates a new {@code Partitioner} instance with the specified schema by table name mapping
     * and {@link BigQueryConfig BigQuery configuration}.
     *
     * @param tablesByName   a mapping of table names to their corresponding schemas.
     * @param bigQueryConfig the BigQuery configuration.
     */
    public static Partitioner partition(Map<String, Table> tablesByName, BigQueryConfig bigQueryConfig) {
        return new Partitioner(tablesByName, bigQueryConfig);
    }

    /**
     * Processes a single element of {@code TableRow} and performs partitioning based on specified criteria.
     *
     * @param row the {@code TableRow} to process and partition.
     * @param out the output receiver for emitting partitioned {@code TableRow}'s.
     */
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
        try {
            TableRow partitionedRow = row.clone();

            long timestampMillis = Long.parseLong(row.get(TS_MS).toString());
            if (isOlderThanPartitionExpiry(timestampMillis)) {
                logger.info("[ETL-STREAM] Discarding record since the source of data older than {}!",
                    TimeUnit.MILLISECONDS.toDays(bigQueryConfig.getPartitionExpiryMillis()));
                return;
            }

            String tableName = row.get(TABLE_NAME).toString();
            Table table = tablesByName.get(tableName);
            table.getTablePartition().ifPresent(tablePartition -> {
                String partitionColumn = tablePartition.getPartitionColumn();
                String sourceColumn = tablePartition.getSourceColumn();
                partitionedRow.set(partitionColumn, extractTimestamp(row, sourceColumn));
            });

            partitionedRow.set(TS_PARTITION, DateTimeUtils.formatEpochMillis(timestampMillis, TIMESTAMP_FORMAT));
            partitionedRow.remove(TS_MS);

            out.output(partitionedRow);
        } catch (Exception ex) {
            logger.error("[ETL-STREAM] Error partitioning TableRow was found! Row: {}", row, ex);
            errorRecords.inc();
        }
    }

    /**
     * Checks if the source timestamp is older than the configured partition expiry time.
     * <p>
     * This method utilizes {@link org.joda.time.DateTimeUtils#setCurrentMillisFixed(long)}
     * to allow setting a fixed point in time for testing purposes. By relying on
     * {@link org.joda.time.DateTimeUtils#currentTimeMillis()}, the method ensures that
     * the timestamp reflects the mocked or fixed current time, making it ideal for
     * deterministic and reproducible tests.
     * </p>
     *
     * @param timestampMillis the source timestamp in milliseconds.
     * @return {@code true} if the source timestamp is older than the partition expiry time, otherwise {@code false}.
     */
    private boolean isOlderThanPartitionExpiry(long timestampMillis) {
        long currentStartOfDay = DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis();
        return timestampMillis <= currentStartOfDay - bigQueryConfig.getPartitionExpiryMillis();
    }

    /**
     * Retrieves the formatted timestamp value from the {@code TableRow} based on the specified source column.
     *
     * @param row          the {@code TableRow} containing the source column.
     * @param sourceColumn the name of the source column.
     * @return the formatted timestamp value as a string, or {@code null} if the source column is not present in the TableRow.
     */
    private String extractTimestamp(TableRow row, String sourceColumn) {
        return row.containsKey(sourceColumn) ?
            DateTimeUtils.formatEpochMillis((long) row.get(sourceColumn), TIMESTAMP_FORMAT) : null;
    }
}