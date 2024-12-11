package com.github.davidch93.etl.stream;

import com.github.davidch93.etl.core.utils.DateTimeUtils;
import com.google.api.services.bigquery.model.*;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;

import java.time.Instant;
import java.util.List;

import static com.github.davidch93.etl.core.constants.MetadataField.*;

public final class DataHelper {

    public static final long WRITE_TIME = Instant.parse("2024-12-11T14:25:30.00Z").toEpochMilli();

    public static final String PROJECT_ID = "github-staging";
    public static final String REGION = "asia-southeast1";
    public static final String DATASET_STREAM_MYSQL = "bronze_stream_mysql";
    public static final String DATASET_STREAM_POSTGRESQL = "bronze_stream_postgresql";
    public static final String DATASET_STREAM_MONGODB = "bronze_stream_mongodb";
    public static final String DATASET_STREAM_DYNAMODB = "bronze_stream_dynamodb";

    public static class InputMySql {

        public static final KafkaRecord<String, String> KAFKA_CREATE_OP = new KafkaRecord<>(
            "mysqlstaging.github_staging.orders", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":2}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "c",
                    "ts_ms": 1733813344632,
                    "before": null,
                    "after": {
                      "id": 2,
                      "amount": 150000,
                      "status": "PAID",
                      "is_expired": false,
                      "created_at": 1733813319170
                    },
                    "source": {
                      "version": "1.0.0",
                      "connector": "MYSQL",
                      "ts_ms": 1733813319670,
                      "name": "mysqlstaging",
                      "db": "github_staging",
                      "table": "orders",
                      "server_id": 20241210,
                      "gtid": "50791c3f-66ae-ee15-4e7a-70668ea778de:14981727984",
                      "file": "binlog-0001",
                      "pos": 4,
                      "xid": 728530626
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_UPDATE_OP = new KafkaRecord<>(
            "mysqlstaging.github_staging.orders", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":2}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "u",
                    "ts_ms": 1733813345632,
                    "before": {
                      "id": 2,
                      "amount": 150000,
                      "status": "PAID",
                      "is_expired": false,
                      "created_at": 1733813319170
                    },
                    "after": {
                      "id": 2,
                      "amount": 150000,
                      "status": "PAID",
                      "is_expired": true,
                      "created_at": 1733813319170
                    },
                    "source": {
                      "version": "1.0.0",
                      "connector": "MYSQL",
                      "ts_ms": 1733813320670,
                      "name": "mysqlstaging",
                      "db": "github_staging",
                      "table": "orders",
                      "server_id": 20241210,
                      "gtid": "50791c3f-66ae-ee15-4e7a-70668ea778de:14981727985",
                      "file": "binlog-0001",
                      "pos": 5,
                      "xid": 728530627
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_DELETE_OP = new KafkaRecord<>(
            "mysqlstaging.github_staging.orders", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":2}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "d",
                    "ts_ms": 1733813346632,
                    "before": {
                      "id": 2,
                      "amount": 150000,
                      "status": "PAID",
                      "is_expired": true,
                      "created_at": 1733813319170
                    },
                    "after": null,
                    "source": {
                      "version": "1.0.0",
                      "connector": "MYSQL",
                      "ts_ms": 1733813321670,
                      "name": "mysqlstaging",
                      "db": "github_staging",
                      "table": "orders",
                      "server_id": 20241210,
                      "gtid": "50791c3f-66ae-ee15-4e7a-70668ea778de:14981727986",
                      "file": "binlog-0001",
                      "pos": 6,
                      "xid": 728530628
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_EXPIRED_PAYLOAD = new KafkaRecord<>(
            "mysqlstaging.github_staging.orders", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":1}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "c",
                    "ts_ms": 1733613319270,
                    "before": null,
                    "after": {
                      "id": 1,
                      "amount": 150000,
                      "status": "PAID",
                      "is_expired": false,
                      "created_at": 1733613319170
                    },
                    "source": {
                      "version": "1.0.0",
                      "connector": "MYSQL",
                      "ts_ms": 1733613319170,
                      "name": "mysqlstaging",
                      "db": "github_staging",
                      "table": "orders",
                      "server_id": 20241210,
                      "gtid": "50791c3f-66ae-ee15-4e7a-70668ea778de:14881727986",
                      "file": "binlog-0000",
                      "pos": 4,
                      "xid": 628530628
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> INVALID_KAFKA_PAYLOAD = new KafkaRecord<>(
            "mysqlstaging.github_staging.orders", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":2}",
            "not-a-json"
        );
    }

    public static class OutputMySql {

        public static final TableRow TABLE_ROW_CREATE_OP = new TableRow()
            .set("id", 2L)
            .set("amount", 150000.0)
            .set("status", "PAID")
            .set("is_expired", false)
            .set("created_at", 1733813319170L)
            .set(IS_DELETED, false)
            .set(OP, "c")
            .set(TS_MS, 1733813319670L)
            .set(FILE, "binlog-0001")
            .set(POS, 4L)
            .set(TABLE_NAME, "github_staging_orders")
            .set(SOURCE, "MYSQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_UPDATE_OP = new TableRow()
            .set("id", 2L)
            .set("amount", 150000.0)
            .set("status", "PAID")
            .set("is_expired", true)
            .set("created_at", 1733813319170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(TS_MS, 1733813320670L)
            .set(FILE, "binlog-0001")
            .set(POS, 5L)
            .set(TABLE_NAME, "github_staging_orders")
            .set(SOURCE, "MYSQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_DELETE_OP = new TableRow()
            .set("id", 2L)
            .set("amount", 150000.0)
            .set("status", "PAID")
            .set("is_expired", true)
            .set("created_at", 1733813319170L)
            .set(IS_DELETED, true)
            .set(OP, "d")
            .set(TS_MS, 1733813321670L)
            .set(FILE, "binlog-0001")
            .set(POS, 6L)
            .set(TABLE_NAME, "github_staging_orders")
            .set(SOURCE, "MYSQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_CREATE_OP = new TableRow()
            .set("id", 2L)
            .set("amount", 150000.0)
            .set("status", "PAID")
            .set("is_expired", false)
            .set("created_at", 1733813319170L)
            .set(IS_DELETED, false)
            .set(OP, "c")
            .set(FILE, "binlog-0001")
            .set(POS, 4L)
            .set(TABLE_NAME, "github_staging_orders")
            .set(SOURCE, "MYSQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set("order_timestamp", DateTimeUtils.formatEpochMillis(1733813319170L, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813319670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_UPDATE_OP = new TableRow()
            .set("id", 2L)
            .set("amount", 150000.0)
            .set("status", "PAID")
            .set("is_expired", true)
            .set("created_at", 1733813319170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(FILE, "binlog-0001")
            .set(POS, 5L)
            .set(TABLE_NAME, "github_staging_orders")
            .set(SOURCE, "MYSQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set("order_timestamp", DateTimeUtils.formatEpochMillis(1733813319170L, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813320670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_DELETE_OP = new TableRow()
            .set("id", 2L)
            .set("amount", 150000.0)
            .set("status", "PAID")
            .set("is_expired", true)
            .set("created_at", 1733813319170L)
            .set(IS_DELETED, true)
            .set(OP, "d")
            .set(FILE, "binlog-0001")
            .set(POS, 6L)
            .set(TABLE_NAME, "github_staging_orders")
            .set(SOURCE, "MYSQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set("order_timestamp", DateTimeUtils.formatEpochMillis(1733813319170L, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813321670L, TIMESTAMP_FORMAT));
    }

    public static class MySqlTable {

        public static final List<TableFieldSchema> STREAM_ORDERS_SCHEMA = List.of(
            new TableFieldSchema()
                .setName("id")
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("Unique identifier for the order"),
            new TableFieldSchema()
                .setName("amount")
                .setType("FLOAT")
                .setMode("NULLABLE")
                .setDescription("Total amount of the order"),
            new TableFieldSchema()
                .setName("status")
                .setType("STRING")
                .setMode("NULLABLE")
                .setDescription("Status of the order"),
            new TableFieldSchema()
                .setName("is_expired")
                .setType("BOOLEAN")
                .setMode("NULLABLE")
                .setDescription("Whether the order expires or not"),
            new TableFieldSchema()
                .setName("created_at")
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("Date of the order"),
            new TableFieldSchema()
                .setName("order_timestamp")
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
                .setDescription("A partition column indicating the date of the order was created"),
            new TableFieldSchema()
                .setName(IS_DELETED)
                .setType("BOOLEAN")
                .setMode("REQUIRED")
                .setDescription("Indicates if the record is marked as deleted."),
            new TableFieldSchema()
                .setName(TABLE_NAME)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The name of the source table."),
            new TableFieldSchema()
                .setName(SOURCE)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The name of the data source (e.g., database)."),
            new TableFieldSchema()
                .setName(OP)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The type of operation performed: 'c' for create, 'u' for update, 'd' for delete."),
            new TableFieldSchema()
                .setName(TS_PARTITION)
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
                .setDescription("The native timestamp of the database transaction logs."),
            new TableFieldSchema()
                .setName(FILE)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The name of the binlog file."),
            new TableFieldSchema()
                .setName(POS)
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("The position in the binlog file."),
            new TableFieldSchema()
                .setName(AUDIT_WRITE_TIME)
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
                .setDescription("The timestamp when the record was written for auditing purposes.")
        );

        public static final Table STREAM_ORDERS = new Table()
            .setTableReference(new TableReference()
                .setProjectId(PROJECT_ID)
                .setDatasetId(DATASET_STREAM_MYSQL)
                .setTableId("github_staging_orders"))
            .setSchema(new TableSchema()
                .setFields(STREAM_ORDERS_SCHEMA))
            .setTimePartitioning(new TimePartitioning()
                .setField(TS_PARTITION)
                .setType("DAY")
                .setExpirationMs(172800000L))
            .setDescription("Schema for the orders table");
    }

    public static class InputPostgreSql {

        public static final KafkaRecord<String, String> KAFKA_CREATE_OP = new KafkaRecord<>(
            "postgresqlstaging.github_staging.users", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":2}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "c",
                    "ts_ms": 1733813344632,
                    "before": null,
                    "after": {
                      "id": 2,
                      "email": "user@example.com",
                      "created_at": 1733813319170,
                      "updated_at": 1733813319170
                    },
                    "source": {
                      "version": "1.0.0",
                      "connector": "POSTGRESQL",
                      "ts_ms": 1733813319670,
                      "name": "postgresqlstaging",
                      "db": "github_staging",
                      "schema": "public",
                      "table": "users",
                      "lsn": 24023128,
                      "txid": 728530626
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_UPDATE_OP = new KafkaRecord<>(
            "postgresqlstaging.github_staging.users", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":2}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "u",
                    "ts_ms": 1733813345632,
                    "before": null,
                    "after": {
                      "id": 2,
                      "email": "user-updated@example.com",
                      "created_at": 1733813319170,
                      "updated_at": 1733813320170
                    },
                    "source": {
                      "version": "1.0.0",
                      "connector": "POSTGRESQL",
                      "ts_ms": 1733813320670,
                      "name": "postgresqlstaging",
                      "db": "github_staging",
                      "schema": "public",
                      "table": "users",
                      "lsn": 24023129,
                      "txid": 728530627
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_DELETE_OP = new KafkaRecord<>(
            "postgresqlstaging.github_staging.users", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":2}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "d",
                    "ts_ms": 1733813346632,
                    "before": null,
                    "after": null,
                    "source": {
                      "version": "1.0.0",
                      "connector": "POSTGRESQL",
                      "ts_ms": 1733813321670,
                      "name": "postgresqlstaging",
                      "db": "github_staging",
                      "schema": "public",
                      "table": "users",
                      "lsn": 24023130,
                      "txid": 728530628
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> INVALID_KAFKA_PAYLOAD = new KafkaRecord<>(
            "postgresqlstaging.github_staging.users", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":2}",
            "not-a-json"
        );
    }

    public static class OutputPostgreSql {

        public static final TableRow TABLE_ROW_CREATE_OP = new TableRow()
            .set("id", 2L)
            .set("email", "user@example.com")
            .set("created_at", 1733813319170L)
            .set("updated_at", 1733813319170L)
            .set(IS_DELETED, false)
            .set(OP, "c")
            .set(TS_MS, 1733813319670L)
            .set(LSN, 24023128L)
            .set(TABLE_NAME, "github_staging_users")
            .set(SOURCE, "POSTGRESQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_UPDATE_OP = new TableRow()
            .set("id", 2L)
            .set("email", "user-updated@example.com")
            .set("created_at", 1733813319170L)
            .set("updated_at", 1733813320170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(TS_MS, 1733813320670L)
            .set(LSN, 24023129L)
            .set(TABLE_NAME, "github_staging_users")
            .set(SOURCE, "POSTGRESQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_DELETE_OP = new TableRow()
            .set("id", 2L)
            .set(IS_DELETED, true)
            .set(OP, "d")
            .set(TS_MS, 1733813321670L)
            .set(LSN, 24023130L)
            .set(TABLE_NAME, "github_staging_users")
            .set(SOURCE, "POSTGRESQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_CREATE_OP = new TableRow()
            .set("id", 2L)
            .set("email", "user@example.com")
            .set("created_at", 1733813319170L)
            .set("updated_at", 1733813319170L)
            .set(IS_DELETED, false)
            .set(OP, "c")
            .set(LSN, 24023128L)
            .set(TABLE_NAME, "github_staging_users")
            .set(SOURCE, "POSTGRESQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813319670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_UPDATE_OP = new TableRow()
            .set("id", 2L)
            .set("email", "user-updated@example.com")
            .set("created_at", 1733813319170L)
            .set("updated_at", 1733813320170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(LSN, 24023129L)
            .set(TABLE_NAME, "github_staging_users")
            .set(SOURCE, "POSTGRESQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813320670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_DELETE_OP = new TableRow()
            .set("id", 2L)
            .set(IS_DELETED, true)
            .set(OP, "d")
            .set(LSN, 24023130L)
            .set(TABLE_NAME, "github_staging_users")
            .set(SOURCE, "POSTGRESQL")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813321670L, TIMESTAMP_FORMAT));
    }

    public static class PostgreSqlTable {

        public static final List<TableFieldSchema> STREAM_USERS_SCHEMA = List.of(
            new TableFieldSchema()
                .setName("id")
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("Unique identifier for the user"),
            new TableFieldSchema()
                .setName("email")
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The email address corresponding to the user"),
            new TableFieldSchema()
                .setName("created_at")
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("Date of the order was created"),
            new TableFieldSchema()
                .setName("updated_at")
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("Date of the order was updated"),
            new TableFieldSchema()
                .setName(IS_DELETED)
                .setType("BOOLEAN")
                .setMode("REQUIRED")
                .setDescription("Indicates if the record is marked as deleted."),
            new TableFieldSchema()
                .setName(TABLE_NAME)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The name of the source table."),
            new TableFieldSchema()
                .setName(SOURCE)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The name of the data source (e.g., database)."),
            new TableFieldSchema()
                .setName(OP)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The type of operation performed: 'c' for create, 'u' for update, 'd' for delete."),
            new TableFieldSchema()
                .setName(TS_PARTITION)
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
                .setDescription("The native timestamp of the database transaction logs."),
            new TableFieldSchema()
                .setName(LSN)
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("The Log Sequence Number (LSN) in PostgreSQL."),
            new TableFieldSchema()
                .setName(AUDIT_WRITE_TIME)
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
                .setDescription("The timestamp when the record was written for auditing purposes.")
        );

        public static final Table STREAM_USERS = new Table()
            .setTableReference(new TableReference()
                .setProjectId(PROJECT_ID)
                .setDatasetId(DATASET_STREAM_POSTGRESQL)
                .setTableId("github_staging_users"))
            .setSchema(new TableSchema()
                .setFields(STREAM_USERS_SCHEMA))
            .setTimePartitioning(new TimePartitioning()
                .setField(TS_PARTITION)
                .setType("DAY")
                .setExpirationMs(172800000L))
            .setDescription("Schema for the users table");
    }

    public static class InputMongoDb {

        public static final KafkaRecord<String, String> KAFKA_CREATE_OP = new KafkaRecord<>(
            "mongodbstaging.github_staging.transactions", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"payload\":{\"id\":\"{\\\"$oid\\\":\\\"630b23db6ef80123d6e72d74\\\"}\"}}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "c",
                    "ts_ms": 1733813344632,
                    "before": null,
                    "after": "{\\"_id\\":{\\"$oid\\":\\"630b23db6ef80123d6e72d74\\"},\\"user_id\\":{\\"$numberInt\\":2},\\"order_id\\":{\\"$numberLong\\":2},\\"fee\\":{\\"$numberDecimal\\":\\"12345.6789\\"},\\"created_at\\":{\\"$date\\":1733813319170},\\"updated_at\\":{\\"$date\\":1733813319170}}",
                    "source": {
                      "version": "1.0.0",
                      "connector": "MONGODB",
                      "ts_ms": 1733813319670,
                      "name": "mongodbstaging",
                      "db": "github_staging",
                      "collection": "transactions",
                      "ord": 31
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_UPDATE_OP_WITH_PATCH_AND_SET = new KafkaRecord<>(
            "mongodbstaging.github_staging.transactions", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"payload\":{\"id\":\"{\\\"$oid\\\":\\\"630b23db6ef80123d6e72d74\\\"}\"}}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "u",
                    "ts_ms": 1733813345632,
                    "before": null,
                    "patch": "{\\"$set\\":{\\"fee\\":{\\"$numberDecimal\\":\\"12345.67891\\"},\\"updated_at\\":{\\"$date\\":1733813320170}}}",
                    "source": {
                      "version": "1.0.0",
                      "connector": "MONGODB",
                      "ts_ms": 1733813320670,
                      "name": "mongodbstaging",
                      "db": "github_staging",
                      "collection": "transactions",
                      "ord": 32
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_UPDATE_OP_WITH_PATCH_ONLY = new KafkaRecord<>(
            "mongodbstaging.github_staging.transactions", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"payload\":{\"id\":\"{\\\"$oid\\\":\\\"630b23db6ef80123d6e72d74\\\"}\"}}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "u",
                    "ts_ms": 1733813346632,
                    "before": null,
                    "patch": "{\\"fee\\":{\\"$numberDecimal\\":\\"12345.67892\\"},\\"updated_at\\":{\\"$date\\":1733813321170}}",
                    "source": {
                      "version": "1.0.0",
                      "connector": "MONGODB",
                      "ts_ms": 1733813321670,
                      "name": "mongodbstaging",
                      "db": "github_staging",
                      "collection": "transactions",
                      "ord": 33
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_UPDATE_OP_WITH_DIFF_I = new KafkaRecord<>(
            "mongodbstaging.github_staging.transactions", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"payload\":{\"id\":\"{\\\"$oid\\\":\\\"630b23db6ef80123d6e72d74\\\"}\"}}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "u",
                    "ts_ms": 1733813347632,
                    "before": null,
                    "patch": "{\\"v\\":2,\\"diff\\":{\\"i\\":{\\"notes\\":\\"example\\"}}}",
                    "source": {
                      "version": "1.0.0",
                      "connector": "MONGODB",
                      "ts_ms": 1733813322670,
                      "name": "mongodbstaging",
                      "db": "github_staging",
                      "collection": "transactions",
                      "ord": 34
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_UPDATE_OP_WITH_DIFF_U = new KafkaRecord<>(
            "mongodbstaging.github_staging.transactions", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"payload\":{\"id\":\"{\\\"$oid\\\":\\\"630b23db6ef80123d6e72d74\\\"}\"}}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "u",
                    "ts_ms": 1733813348632,
                    "before": null,
                    "patch": "{\\"v\\":2,\\"diff\\":{\\"u\\":{\\"updated_at\\":{\\"$date\\":1733813322170}}}}",
                    "source": {
                      "version": "1.0.0",
                      "connector": "MONGODB",
                      "ts_ms": 1733813323670,
                      "name": "mongodbstaging",
                      "db": "github_staging",
                      "collection": "transactions",
                      "ord": 35
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_UPDATE_OP_WITH_DIFF_U_AND_I = new KafkaRecord<>(
            "mongodbstaging.github_staging.transactions", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"payload\":{\"id\":\"{\\\"$oid\\\":\\\"630b23db6ef80123d6e72d74\\\"}\"}}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "u",
                    "ts_ms": 1733813349632,
                    "before": null,
                    "patch": "{\\"v\\":2,\\"diff\\":{\\"u\\":{\\"updated_at\\":{\\"$date\\":1733813323170}},\\"i\\":{\\"notes\\":{\\"user\\":\\"data\\",\\"status\\":\\"PAID\\"}}}}",
                    "source": {
                      "version": "1.0.0",
                      "connector": "MONGODB",
                      "ts_ms": 1733813324670,
                      "name": "mongodbstaging",
                      "db": "github_staging",
                      "collection": "transactions",
                      "ord": 36
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> KAFKA_DELETE_OP = new KafkaRecord<>(
            "mongodbstaging.github_staging.transactions", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"payload\":{\"id\":\"{\\\"$oid\\\":\\\"630b23db6ef80123d6e72d74\\\"}\"}}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "d",
                    "ts_ms": 1733813350632,
                    "before": null,
                    "after": null,
                    "source": {
                      "version": "1.0.0",
                      "connector": "MONGODB",
                      "ts_ms": 1733813325670,
                      "name": "mongodbstaging",
                      "db": "github_staging",
                      "collection": "transactions",
                      "ord": 37
                    }
                  }
                }
                """.replaceAll("\n", "")
        );

        public static final KafkaRecord<String, String> INVALID_KAFKA_PAYLOAD = new KafkaRecord<>(
            "mongodbstaging.github_staging.transactions", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"payload\":{\"id\":\"{\\\"$oid\\\":\\\"630b23db6ef80123d6e72d74\\\"}\"}}",
            "not-a-json"
        );
    }

    public static class OutputMongoDb {

        public static final TableRow TABLE_ROW_CREATE_OP = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("user_id", 2L)
            .set("order_id", 2L)
            .set("fee", 12345.6789)
            .set("created_at", 1733813319170L)
            .set("updated_at", 1733813319170L)
            .set(IS_DELETED, false)
            .set(OP, "c")
            .set(TS_MS, 1733813319670L)
            .set(ORD, 31L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_UPDATE_OP_WITH_PATCH_AND_SET = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("fee", 12345.67891)
            .set("updated_at", 1733813320170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(TS_MS, 1733813320670L)
            .set(ORD, 32L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_UPDATE_OP_WITH_PATCH_ONLY = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("fee", 12345.67892)
            .set("updated_at", 1733813321170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(TS_MS, 1733813321670L)
            .set(ORD, 33L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_UPDATE_OP_WITH_DIFF_I = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("notes", "example")
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(TS_MS, 1733813322670L)
            .set(ORD, 34L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_UPDATE_OP_WITH_DIFF_U = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("updated_at", 1733813322170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(TS_MS, 1733813323670L)
            .set(ORD, 35L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_UPDATE_OP_WITH_DIFF_U_AND_I = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("notes", "{\"user\":\"data\",\"status\":\"PAID\"}")
            .set("updated_at", 1733813323170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(TS_MS, 1733813324670L)
            .set(ORD, 36L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow TABLE_ROW_DELETE_OP = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set(IS_DELETED, true)
            .set(OP, "d")
            .set(TS_MS, 1733813325670L)
            .set(ORD, 37L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_CREATE_OP = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("user_id", 2L)
            .set("order_id", 2L)
            .set("fee", 12345.6789)
            .set("created_at", 1733813319170L)
            .set("updated_at", 1733813319170L)
            .set(IS_DELETED, false)
            .set(OP, "c")
            .set(ORD, 31L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813319670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_PATCH_AND_SET = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("fee", 12345.67891)
            .set("updated_at", 1733813320170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(ORD, 32L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813320670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_PATCH_ONLY = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("fee", 12345.67892)
            .set("updated_at", 1733813321170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(ORD, 33L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813321670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_DIFF_I = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("notes", "example")
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(ORD, 34L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813322670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_DIFF_U = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("updated_at", 1733813322170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(ORD, 35L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813323670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_DIFF_U_AND_I = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set("notes", "{\"user\":\"data\",\"status\":\"PAID\"}")
            .set("updated_at", 1733813323170L)
            .set(IS_DELETED, false)
            .set(OP, "u")
            .set(ORD, 36L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813324670L, TIMESTAMP_FORMAT));

        public static final TableRow PARTITIONED_TABLE_ROW_DELETE_OP = new TableRow()
            .set("_id", "{\"$oid\":\"630b23db6ef80123d6e72d74\"}")
            .set(IS_DELETED, true)
            .set(OP, "d")
            .set(ORD, 37L)
            .set(TABLE_NAME, "github_staging_transactions")
            .set(SOURCE, "MONGODB")
            .set(AUDIT_WRITE_TIME, DateTimeUtils.formatEpochMillis(WRITE_TIME, TIMESTAMP_FORMAT))
            .set(TS_PARTITION, DateTimeUtils.formatEpochMillis(1733813325670L, TIMESTAMP_FORMAT));
    }

    public static class MongoDbTable {

        public static final List<TableFieldSchema> STREAM_TRANSACTIONS_SCHEMA = List.of(
            new TableFieldSchema()
                .setName("_id")
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("Unique identifier for the transaction"),
            new TableFieldSchema()
                .setName("user_id")
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("The id corresponding to the user table"),
            new TableFieldSchema()
                .setName("order_id")
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("The id corresponding to the order table"),
            new TableFieldSchema()
                .setName("fee")
                .setType("FLOAT")
                .setMode("NULLABLE")
                .setDescription("The transaction fee"),
            new TableFieldSchema()
                .setName("notes")
                .setType("STRING")
                .setMode("NULLABLE")
                .setDescription("The transaction notes"),
            new TableFieldSchema()
                .setName("created_at")
                .setType("INTEGER")
                .setMode("NULLABLE")
                .setDescription("Date of the order was created"),
            new TableFieldSchema()
                .setName("updated_at")
                .setType("INTEGER")
                .setMode("NULLABLE")
                .setDescription("Date of the order was updated"),
            new TableFieldSchema()
                .setName(IS_DELETED)
                .setType("BOOLEAN")
                .setMode("REQUIRED")
                .setDescription("Indicates if the record is marked as deleted."),
            new TableFieldSchema()
                .setName(TABLE_NAME)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The name of the source table."),
            new TableFieldSchema()
                .setName(SOURCE)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The name of the data source (e.g., database)."),
            new TableFieldSchema()
                .setName(OP)
                .setType("STRING")
                .setMode("REQUIRED")
                .setDescription("The type of operation performed: 'c' for create, 'u' for update, 'd' for delete."),
            new TableFieldSchema()
                .setName(TS_PARTITION)
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
                .setDescription("The native timestamp of the database transaction logs."),
            new TableFieldSchema()
                .setName(ORD)
                .setType("INTEGER")
                .setMode("REQUIRED")
                .setDescription("The order number in MongoDB's Oplog."),
            new TableFieldSchema()
                .setName(AUDIT_WRITE_TIME)
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
                .setDescription("The timestamp when the record was written for auditing purposes.")
        );

        public static final Table STREAM_TRANSACTIONS = new Table()
            .setTableReference(new TableReference()
                .setProjectId(PROJECT_ID)
                .setDatasetId(DATASET_STREAM_MONGODB)
                .setTableId("github_staging_transactions"))
            .setSchema(new TableSchema()
                .setFields(STREAM_TRANSACTIONS_SCHEMA))
            .setTimePartitioning(new TimePartitioning()
                .setField(TS_PARTITION)
                .setType("DAY")
                .setExpirationMs(172800000L))
            .setDescription("Schema for the transactions table");
    }

    public static class InputDynamoDb {

        public static final KafkaRecord<String, String> KAFKA_CREATE_OP = new KafkaRecord<>(
            "dynamodbstaging.github_staging.transactions", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null,
            "{\"id\":2}",
            """
                {
                  "schema": [],
                  "payload": {
                    "op": "c",
                    "ts_ms": 1733813347632,
                    "before": null,
                    "after": {
                      "id": 2,
                      "amount": 150000,
                      "status": "PAID",
                      "is_expired": false,
                      "created_at": 1733813325170
                    },
                    "source": {
                      "version": "1.0.0",
                      "connector": "DYNAMODB",
                      "ts_ms": 1733813325270,
                      "name": "dynamodbstaging",
                      "db": "github_staging",
                      "table": "transactions",
                      "shard_id": "shardId-00000001696411483678-f3df68e5",
                      "sequence_number": "600000000019036501897",
                    }
                  }
                }
                """.replaceAll("\n", "")
        );
    }
}
