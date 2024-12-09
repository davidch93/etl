package com.github.davidch93.etl.stream;

import com.google.api.services.bigquery.model.*;

import java.util.List;

import static com.github.davidch93.etl.core.constants.MetadataField.*;

public final class DataHelper {

    public static final String PROJECT_ID = "github-staging";
    public static final String REGION = "asia-southeast1";
    public static final String DATASET_STREAM_MYSQL = "bronze_stream_mysql";
    public static final String DATASET_STREAM_POSTGRESQL = "bronze_stream_postgresql";
    public static final String DATASET_STREAM_MONGODB = "bronze_stream_mongodb";
    public static final String DATASET_STREAM_DYNAMODB = "bronze_stream_dynamodb";

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
}
