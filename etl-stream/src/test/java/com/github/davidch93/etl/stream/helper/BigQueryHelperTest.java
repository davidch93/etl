package com.github.davidch93.etl.stream.helper;

import com.github.davidch93.etl.core.config.BigQueryConfig;
import com.github.davidch93.etl.core.schema.SchemaLoader;
import com.github.davidch93.etl.core.schema.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.github.davidch93.etl.stream.DataHelper.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BigQueryHelperTest {

    private static final FakeDatasetService fakeDatasetService = new FakeDatasetService();
    private static final FakeJobService fakeJobService = new FakeJobService();
    private static final FakeBigQueryServices fakeBigQueryServices = new FakeBigQueryServices()
        .withDatasetService(fakeDatasetService)
        .withJobService(fakeJobService);

    private static BigQueryHelper bigQueryHelper;

    @BeforeAll
    static void setup() throws IOException, InterruptedException {
        FakeDatasetService.setUp();

        fakeDatasetService.createDataset(PROJECT_ID, DATASET_STREAM_MYSQL, REGION, "", 0L);
        fakeDatasetService.createDataset(PROJECT_ID, DATASET_STREAM_POSTGRESQL, REGION, "", 0L);
        fakeDatasetService.createDataset(PROJECT_ID, DATASET_STREAM_MONGODB, REGION, "", 0L);
        fakeDatasetService.createDataset(PROJECT_ID, DATASET_STREAM_DYNAMODB, REGION, "", 0L);

        BigQueryOptions options = PipelineOptionsFactory.create().as(BigQueryOptions.class);
        BigQueryConfig bigQueryConfig = new BigQueryConfig();
        bigQueryConfig.setProjectId(PROJECT_ID);
        bigQueryConfig.setRegion(REGION);
        bigQueryConfig.setDatasetId("bronze");
        bigQueryConfig.setCreateDisposition("CREATE_IF_NEEDED");
        bigQueryConfig.setPartitionExpiryMillis(172800000L);
        bigQueryConfig.setTemporaryGcsBucket("gs://temp");

        bigQueryHelper = new BigQueryHelper(fakeBigQueryServices, options, bigQueryConfig);
    }

    @AfterAll
    static void tearDown() throws IOException, InterruptedException {
        fakeDatasetService.deleteDataset(PROJECT_ID, DATASET_STREAM_MYSQL);
        fakeDatasetService.deleteDataset(PROJECT_ID, DATASET_STREAM_POSTGRESQL);
        fakeDatasetService.deleteDataset(PROJECT_ID, DATASET_STREAM_MONGODB);
        fakeDatasetService.deleteDataset(PROJECT_ID, DATASET_STREAM_DYNAMODB);
    }

    /**
     * The unit test below expects a successful query job but encounters a BaseEncoding error.
     * However, the functionality test below has succeeded in Google Dataflow.
     */
    @Test
    void testPreparePipelineForMySql_whenStreamTableNotExist_expectTableAndViewCreated() throws Exception {
        String schemaFilePath = "src/test/resources/schema/mysql/github_staging_orders/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);

        String realTimeViewQuery = """
            CREATE OR REPLACE VIEW `github-staging.bronze_real_time_mysql.github_staging_orders` AS (
            WITH
              stream AS (
                SELECT `id`, `amount`, `status`, `is_expired`, `created_at`, `order_timestamp`, `_is_deleted` FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `_ts_partition` DESC, `_file` DESC, `_pos` DESC) AS row_number
                  FROM `github-staging.bronze_stream_mysql.github_staging_orders`)
                WHERE row_number = 1),
              daily AS (
                SELECT `id`, `amount`, `status`, `is_expired`, `created_at`, `order_timestamp`, `_is_deleted`
                FROM `github-staging.bronze_daily_mysql.github_staging_orders`)
            SELECT stream.* FROM stream
            UNION ALL
            SELECT daily.* FROM daily
            LEFT JOIN (SELECT `id` FROM stream) distinct_stream
            ON daily.`id` = distinct_stream.`id`
            WHERE distinct_stream.`id` IS NULL
            )
            """.strip();

        assertThatThrownBy(() -> bigQueryHelper.prepareStreamTablesAndRealTimeViews(List.of(table)))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining(realTimeViewQuery);

        // Assert the stream table creation
        TableReference tableReference = new TableReference()
            .setProjectId(PROJECT_ID)
            .setDatasetId(DATASET_STREAM_MYSQL)
            .setTableId(table.getName());
        com.google.api.services.bigquery.model.Table streamTable = fakeDatasetService.getTable(tableReference);

        assertThat(streamTable).isNotNull();
        assertThat(streamTable).isEqualTo(MySqlTable.STREAM_ORDERS);

        // Delete the stream table
        fakeDatasetService.deleteTable(tableReference);
    }

    /**
     * The unit test below expects a successful query job but encounters a BaseEncoding error.
     * However, the functionality test below has succeeded in Google Dataflow.
     */
    @Test
    void testPreparePipelineForMySql_whenStreamTableExists_expectStreamTableAltered() throws Exception {
        // Create a stream table
        TableReference tableReference = new TableReference()
            .setProjectId(PROJECT_ID)
            .setDatasetId(DATASET_STREAM_MYSQL)
            .setTableId("github_staging_orders");
        com.google.api.services.bigquery.model.Table streamOrders = new com.google.api.services.bigquery.model.Table()
            .setTableReference(tableReference)
            .setSchema(new TableSchema()
                .setFields(List.of(
                    new TableFieldSchema()
                        .setName("id")
                        .setType("INTEGER")
                        .setMode("REQUIRED")
                        .setDescription("Unique identifier for the order"),
                    new TableFieldSchema()
                        .setName("updated_at")
                        .setType("INTEGER")
                        .setMode("NULLABLE")
                        .setDescription("Date of the order was updated")
                )))
            .setDescription("Schema for the orders table");
        fakeDatasetService.createTable(streamOrders);

        String schemaFilePath = "src/test/resources/schema/mysql/github_staging_orders/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);

        String alterQuery = """
            ALTER TABLE `github-staging.bronze_stream_mysql.github_staging_orders`
            ADD COLUMN `amount` FLOAT, ADD COLUMN `status` STRING,
            ADD COLUMN `is_expired` BOOLEAN,
            ADD COLUMN `created_at` INTEGER,
            ADD COLUMN `order_timestamp` TIMESTAMP,
            ADD COLUMN `_is_deleted` BOOLEAN,
            ADD COLUMN `_table_name` STRING,
            ADD COLUMN `_source` STRING,
            ADD COLUMN `_op` STRING,
            ADD COLUMN `_ts_partition` TIMESTAMP,
            ADD COLUMN `_file` STRING,
            ADD COLUMN `_pos` INTEGER,
            ADD COLUMN `_audit_write_time` TIMESTAMP,
            DROP COLUMN `updated_at`
            """.strip().replaceAll("\n", " ");

        assertThatThrownBy(() -> bigQueryHelper.prepareStreamTablesAndRealTimeViews(List.of(table)))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining(alterQuery);

        // Delete the stream table
        fakeDatasetService.deleteTable(tableReference);
    }

    /**
     * The unit test below expects a successful query job but encounters a BaseEncoding error.
     * However, the functionality test below has succeeded in Google Dataflow.
     */
    @Test
    void testPreparePipelineForPostgreSql_whenStreamTableNotExist_expectTableAndViewCreated() throws Exception {
        String schemaFilePath = "src/test/resources/schema/postgresql/github_staging_users/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);

        String realTimeViewQuery = """
            CREATE OR REPLACE VIEW `github-staging.bronze_real_time_postgresql.github_staging_users` AS (
            WITH
              stream AS (
                SELECT `id`, `email`, `created_at`, `updated_at`, `_is_deleted` FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `_ts_partition` DESC, `_lsn` DESC) AS row_number
                  FROM `github-staging.bronze_stream_postgresql.github_staging_users`)
                WHERE row_number = 1),
              daily AS (
                SELECT `id`, `email`, `created_at`, `updated_at`, `_is_deleted`
                FROM `github-staging.bronze_daily_postgresql.github_staging_users`)
            SELECT stream.* FROM stream
            UNION ALL
            SELECT daily.* FROM daily
            LEFT JOIN (SELECT `id` FROM stream) distinct_stream
            ON daily.`id` = distinct_stream.`id`
            WHERE distinct_stream.`id` IS NULL
            )
            """.strip();

        assertThatThrownBy(() -> bigQueryHelper.prepareStreamTablesAndRealTimeViews(List.of(table)))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining(realTimeViewQuery);

        // Assert the stream table creation
        TableReference tableReference = new TableReference()
            .setProjectId(PROJECT_ID)
            .setDatasetId(DATASET_STREAM_POSTGRESQL)
            .setTableId(table.getName());
        com.google.api.services.bigquery.model.Table streamTable = fakeDatasetService.getTable(tableReference);

        assertThat(streamTable).isNotNull();
        assertThat(streamTable).isEqualTo(PostgreSqlTable.STREAM_USERS);

        // Delete the stream table
        fakeDatasetService.deleteTable(tableReference);
    }

    @Test
    void testPreparePipelineForPostgreSql_whenStreamTableExistWithSameSchema_expectDoNothing() throws Exception {
        // Create a stream table
        fakeDatasetService.createTable(PostgreSqlTable.STREAM_USERS);

        String schemaFilePath = "src/test/resources/schema/postgresql/github_staging_users/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);

        bigQueryHelper.prepareStreamTablesAndRealTimeViews(List.of(table));

        // Delete the stream table
        TableReference tableReference = new TableReference()
            .setProjectId(PROJECT_ID)
            .setDatasetId(DATASET_STREAM_MYSQL)
            .setTableId("github_staging_orders");
        fakeDatasetService.deleteTable(tableReference);
    }

    /**
     * The unit test below expects a successful query job but encounters a BaseEncoding error.
     * However, the functionality test below has succeeded in Google Dataflow.
     */
    @Test
    void testPreparePipelineForMongoDb_whenStreamTableNotExist_expectTableAndViewCreated() throws Exception {
        String schemaFilePath = "src/test/resources/schema/mongodb/github_staging_transactions/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);

        String realTimeViewQuery = """
            CREATE OR REPLACE VIEW `github-staging.bronze_real_time_mongodb.github_staging_transactions` AS (
            WITH
              union_stream AS (
                SELECT `_id`, `user_id`, `order_id`, `fee`, `notes`, `created_at`, `updated_at`, `_is_deleted`, `_ts_partition`, `_ord`
                FROM `github-staging.bronze_stream_mongodb.github_staging_transactions`
                UNION ALL
                SELECT `_id`, `user_id`, `order_id`, `fee`, `notes`, `created_at`, `updated_at`, `_is_deleted`, '1970-01-01' AS `_ts_partition`, 0 AS `_ord`
                FROM `github-staging.bronze_daily_mongodb.github_staging_transactions`
                WHERE `_id`, `user_id`, `order_id` IN (SELECT `_id`, `user_id`, `order_id` FROM `github-staging.bronze_stream_mongodb.github_staging_transactions`)),
              distinct_stream AS (
                SELECT `_id`, `user_id`, `order_id`, ARRAY_AGG(`fee` IGNORE NULLS ORDER BY `_ts_partition` DESC, `_ord` DESC LIMIT 1)[OFFSET(0)] AS `fee`,
            ARRAY_AGG(`notes` IGNORE NULLS ORDER BY `_ts_partition` DESC, `_ord` DESC LIMIT 1)[OFFSET(0)] AS `notes`,
            ARRAY_AGG(`created_at` IGNORE NULLS ORDER BY `_ts_partition` DESC, `_ord` DESC LIMIT 1)[OFFSET(0)] AS `created_at`,
            ARRAY_AGG(`updated_at` IGNORE NULLS ORDER BY `_ts_partition` DESC, `_ord` DESC LIMIT 1)[OFFSET(0)] AS `updated_at`,
            ARRAY_AGG(`_is_deleted` IGNORE NULLS ORDER BY `_ts_partition` DESC, `_ord` DESC LIMIT 1)[OFFSET(0)] AS `_is_deleted` FROM union_stream GROUP BY 1),
              daily AS (
                SELECT `_id`, `user_id`, `order_id`, `fee`, `notes`, `created_at`, `updated_at`, `_is_deleted`
                FROM `github-staging.bronze_daily_mongodb.github_staging_transactions`
                WHERE `_id`, `user_id`, `order_id` NOT IN (
                  SELECT `_id`, `user_id`, `order_id` FROM distinct_stream))
            SELECT * FROM distinct_stream
            UNION ALL
            SELECT * FROM daily
            )
            """.strip();

        assertThatThrownBy(() -> bigQueryHelper.prepareStreamTablesAndRealTimeViews(List.of(table)))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining(realTimeViewQuery);

        // Assert the stream table creation
        TableReference tableReference = new TableReference()
            .setProjectId(PROJECT_ID)
            .setDatasetId(DATASET_STREAM_MONGODB)
            .setTableId(table.getName());
        com.google.api.services.bigquery.model.Table streamTable = fakeDatasetService.getTable(tableReference);

        assertThat(streamTable).isNotNull();
        assertThat(streamTable).isEqualTo(MongoDbTable.STREAM_TRANSACTIONS);

        // Delete the stream table
        fakeDatasetService.deleteTable(tableReference);
    }

    @Test
    void testPreparePipelineForUnsupportedSource_expectThrowException() {
        String schemaFilePath = "src/test/resources/schema/dynamodb/github_staging_transactions/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);

        assertThatThrownBy(() -> bigQueryHelper.prepareStreamTablesAndRealTimeViews(List.of(table)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported source type for `DYNAMODB`!");
    }
}
