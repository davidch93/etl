package com.github.davidch93.etl.core.constants;

import java.util.List;

/**
 * A class containing constants for metadata column names used in various ETL operations.
 *
 * <p>
 * This class organizes metadata column names by context, including Kafka, Debezium, ETL-specific,
 * and ETL job history. These constants provide a standardized way to reference metadata columns
 * in the ETL pipeline and related processes.
 * </p>
 *
 * <h2>Contexts:</h2>
 * <ul>
 *     <li><strong>Kafka:</strong> Metadata related to Kafka records.</li>
 *     <li><strong>Debezium:</strong> Metadata columns produced by Debezium for CDC (Change Data Capture).</li>
 *     <li><strong>ETL:</strong> Metadata added or used by ETL operations.</li>
 *     <li><strong>ETL Job History:</strong> Metadata for tracking the status and details of ETL jobs.</li>
 * </ul>
 *
 * <p><strong>Note:</strong> This class is immutable and all fields are declared as {@code static final} for ease of use.</p>
 *
 * @author david.christianto
 */
public final class MetadataField {

    // Kafka
    public static final String KEY = "key";
    public static final String VALUE = "value";

    // Debezium
    public static final String PAYLOAD = "payload";
    public static final String PAYLOAD_OP = "op";
    public static final String PAYLOAD_TS_MS = "ts_ms";
    public static final String PAYLOAD_BEFORE = "before";
    public static final String PAYLOAD_AFTER = "after";
    public static final String PAYLOAD_PATCH = "patch";
    public static final String PAYLOAD_SET = "$set";
    public static final String PAYLOAD_DIFF = "diff";
    public static final String PAYLOAD_DIFF_I = "i";
    public static final String PAYLOAD_DIFF_U = "u";
    public static final String PAYLOAD_SOURCE = "source";
    public static final String PAYLOAD_SOURCE_TS_MS = "ts_ms";
    public static final String PAYLOAD_SOURCE_FILE = "file";
    public static final String PAYLOAD_SOURCE_POS = "pos";
    public static final String PAYLOAD_SOURCE_ORD = "ord";
    public static final String PAYLOAD_SOURCE_LSN = "lsn";
    public static final String PAYLOAD_SOURCE_SHARD_ID = "shard_id";
    public static final String PAYLOAD_SOURCE_SEQUENCE_NUMBER = "sequence_number";

    // ETL
    public static final String IS_DELETED = "_is_deleted";
    public static final String TABLE_NAME = "_table_name";
    public static final String SOURCE = "_source";
    public static final String OP = "_op";
    public static final String TS_MS = "_ts_ms";
    public static final String TS_PARTITION = "_ts_partition";
    public static final String FILE = "_file";
    public static final String POS = "_pos";
    public static final String ORD = "_ord";
    public static final String LSN = "_lsn";
    public static final String AUDIT_WRITE_TIME = "_audit_write_time";

    // ETL Job History
    public static final String BQ_TABLE_NAME = "bq_table_name";
    public static final String GROUP_NAME = "group_name";
    public static final String SCHEDULED_TIMESTAMP = "scheduled_timestamp";
    public static final String START_TIMESTAMP = "start_timestamp";
    public static final String END_TIMESTAMP = "end_timestamp";
    public static final String DURATION_SECONDS = "duration_seconds";
    public static final String IS_SUCCESS = "is_success";
    public static final String MESSAGE = "message";
}