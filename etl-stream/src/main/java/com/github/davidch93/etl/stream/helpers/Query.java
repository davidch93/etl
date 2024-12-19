package com.github.davidch93.etl.stream.helpers;

import com.google.api.services.bigquery.model.TableFieldSchema;

import java.util.List;

import static com.github.davidch93.etl.core.constants.MetadataField.*;

/**
 * Utility class for generating SQL queries for managing and preparing streams and real-time tables in BigQuery.
 * <p>
 * This class provides methods for constructing SQL queries to:
 * <ul>
 *     <li>Add or drop columns in BigQuery tables</li>
 *     <li>Alter table schema</li>
 *     <li>Create views for MySQL, PostgreSQL, and MongoDB-based pipelines</li>
 *     <li>Generate common SQL patterns like ARRAY_AGG, JOIN conditions, and WHERE filters</li>
 * </ul>
 * </p>
 *
 * @author david.christianto
 */
public final class Query {

    public static final List<TableFieldSchema> MYSQL_METADATA_FIELDS = List.of(
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

    public static final List<TableFieldSchema> POSTGRESQL_METADATA_FIELDS = List.of(
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

    public static final List<TableFieldSchema> MONGODB_METADATA_FIELDS = List.of(
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

    private static final String ADD_COLUMN_QUERY = "ADD COLUMN `%s` %s";
    private static final String DROP_COLUMN_QUERY = "DROP COLUMN `%s`";
    private static final String ALTER_TABLE_QUERY = "ALTER TABLE `%s` %s";
    private static final String ARRAY_AGG_QUERY =
        "ARRAY_AGG(`%s` IGNORE NULLS ORDER BY `" + TS_PARTITION + "` DESC, `" + ORD + "` DESC LIMIT 1)[OFFSET(0)] AS `%s`";
    private static final String JOIN_TABLE = "daily.`%s` = distinct_stream.`%s`";
    private static final String WHERE_FILTER = "distinct_stream.`%s` IS NULL";
    private static final String MYSQL_VIEW = "CREATE OR REPLACE VIEW `%s` AS (\n" +
        "WITH\n" +
        "  stream AS (\n" +
        "    SELECT `%s` FROM (\n" +
        "      SELECT *, ROW_NUMBER() OVER (PARTITION BY `%s` ORDER BY `" + TS_PARTITION + "` DESC, `" + FILE + "` DESC, `" + POS + "` DESC) AS row_number\n" +
        "      FROM `%s`)\n" +
        "    WHERE row_number = 1),\n" +
        "  daily AS (\n" +
        "    SELECT `%s`\n" +
        "    FROM `%s`)\n" +
        "SELECT stream.* FROM stream\n" +
        "UNION ALL\n" +
        "SELECT daily.* FROM daily\n" +
        "LEFT JOIN (SELECT `%s` FROM stream) distinct_stream\n" +
        "ON %s\n" +
        "WHERE %s\n" +
        ")";
    private static final String POSTGRES_VIEW = "CREATE OR REPLACE VIEW `%s` AS (\n" +
        "WITH\n" +
        "  stream AS (\n" +
        "    SELECT `%s` FROM (\n" +
        "      SELECT *, ROW_NUMBER() OVER (PARTITION BY `%s` ORDER BY `" + TS_PARTITION + "` DESC, `" + LSN + "` DESC) AS row_number\n" +
        "      FROM `%s`)\n" +
        "    WHERE row_number = 1),\n" +
        "  daily AS (\n" +
        "    SELECT `%s`\n" +
        "    FROM `%s`)\n" +
        "SELECT stream.* FROM stream\n" +
        "UNION ALL\n" +
        "SELECT daily.* FROM daily\n" +
        "LEFT JOIN (SELECT `%s` FROM stream) distinct_stream\n" +
        "ON %s\n" +
        "WHERE %s\n" +
        ")";
    private static final String MONGO_VIEW = "CREATE OR REPLACE VIEW `%s` AS (\n" +
        "WITH\n" +
        "  union_stream AS (\n" +
        "    SELECT `%s`, `" + TS_PARTITION + "`, `" + ORD + "`\n" +
        "    FROM `%s`\n" +
        "    UNION ALL\n" +
        "    SELECT `%s`, '1970-01-01' AS `" + TS_PARTITION + "`, 0 AS `" + ORD + "`\n" +
        "    FROM `%s`\n" +
        "    WHERE `%s` IN (SELECT `%s` FROM `%s`)),\n" +
        "  distinct_stream AS (\n" +
        "    SELECT `%s`, %s FROM union_stream GROUP BY 1),\n" +
        "  daily AS (\n" +
        "    SELECT `%s`\n" +
        "    FROM `%s`\n" +
        "    WHERE `%s` NOT IN (\n" +
        "      SELECT `%s` FROM distinct_stream))\n" +
        "SELECT * FROM distinct_stream\n" +
        "UNION ALL\n" +
        "SELECT * FROM daily\n" +
        ")";

    /**
     * Generates a SQL query to add a column to a BigQuery table.
     *
     * @param columnName the name of the column to add.
     * @param dataType   the data type of the column.
     * @return the formatted SQL query string.
     */
    public static String addColumn(String columnName, String dataType) {
        return ADD_COLUMN_QUERY.formatted(columnName, dataType);
    }

    /**
     * Generates a SQL query to drop a column from a BigQuery table.
     *
     * @param columnName the name of the column to drop.
     * @return the formatted SQL query string.
     */
    public static String dropColumn(String columnName) {
        return DROP_COLUMN_QUERY.formatted(columnName);
    }

    /**
     * Generates a SQL query to alter a BigQuery table.
     *
     * @param tableName the name of the table to alter.
     * @param queries   the schema modification commands (e.g., add or drop column).
     * @return the formatted SQL query string.
     */
    public static String alterTable(String tableName, String queries) {
        return ALTER_TABLE_QUERY.formatted(tableName, queries);
    }

    /**
     * Generates a SQL query for ARRAY_AGG aggregation with custom ordering.
     *
     * @param columnName the name of the column to aggregate.
     * @return the formatted SQL query string.
     */
    public static String arrayAgg(String columnName) {
        return ARRAY_AGG_QUERY.formatted(columnName, columnName);
    }

    /**
     * Generates a SQL query for JOIN conditions.
     *
     * @param columnName the name of the column used in the JOIN condition.
     * @return the formatted SQL query string.
     */
    public static String joinTable(String columnName) {
        return JOIN_TABLE.formatted(columnName, columnName);
    }

    /**
     * Generates a SQL query for a WHERE filter condition.
     *
     * @param columnName the name of the column used in the filter condition.
     * @return the formatted SQL query string.
     */
    public static String whereFilter(String columnName) {
        return WHERE_FILTER.formatted(columnName);
    }

    /**
     * Generates a MySQL-compatible SQL query to create or replace a view.
     *
     * @param dailyTableName    the name of the daily table.
     * @param streamTableName   the name of the stream table.
     * @param realTimeTableName the name of the resulting view.
     * @param selectPattern     the column selection pattern.
     * @param partitionPattern  the partitioning logic.
     * @param joinPattern       the JOIN condition.
     * @param wherePattern      the WHERE filter condition.
     * @return the formatted SQL query string.
     */
    public static String mysqlView(String dailyTableName, String streamTableName, String realTimeTableName,
                                   String selectPattern, String partitionPattern, String joinPattern,
                                   String wherePattern) {
        return MYSQL_VIEW.formatted(realTimeTableName, selectPattern, partitionPattern, streamTableName,
            selectPattern, dailyTableName, partitionPattern, joinPattern, wherePattern);
    }

    /**
     * Generates a PostgreSQL-compatible SQL query to create or replace a view.
     *
     * @param dailyTableName    the name of the daily table.
     * @param streamTableName   the name of the stream table.
     * @param realTimeTableName the name of the resulting view.
     * @param selectPattern     the column selection pattern.
     * @param partitionPattern  the partitioning logic.
     * @param joinPattern       the JOIN condition.
     * @param wherePattern      the WHERE filter condition.
     * @return the formatted SQL query string.
     */
    public static String postgresView(String dailyTableName, String streamTableName, String realTimeTableName,
                                      String selectPattern, String partitionPattern, String joinPattern,
                                      String wherePattern) {
        return POSTGRES_VIEW.formatted(realTimeTableName, selectPattern, partitionPattern, streamTableName,
            selectPattern, dailyTableName, partitionPattern, joinPattern, wherePattern);
    }

    /**
     * Generates a MongoDB-compatible SQL query to create or replace a view.
     *
     * @param dailyTableName    the name of the daily table.
     * @param streamTableName   the name of the stream table.
     * @param realTimeTableName the name of the resulting view.
     * @param selectPattern     the column selection pattern.
     * @param partitionPattern  the partitioning logic.
     * @param aggregatePattern  the aggregation condition.
     * @return the formatted SQL query string.
     */
    public static String mongoView(String dailyTableName, String streamTableName, String realTimeTableName,
                                   String selectPattern, String partitionPattern, String aggregatePattern) {
        return MONGO_VIEW.formatted(realTimeTableName, selectPattern, streamTableName, selectPattern,
            dailyTableName, partitionPattern, partitionPattern, streamTableName, partitionPattern, aggregatePattern,
            selectPattern, dailyTableName, partitionPattern, partitionPattern);
    }
}
