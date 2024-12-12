package com.github.davidch93.etl.stream.transformers;

import com.github.davidch93.etl.core.config.BigQueryConfig;
import com.github.davidch93.etl.core.schema.SchemaLoader;
import com.github.davidch93.etl.core.schema.Table;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.DateTimeUtils;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.davidch93.etl.stream.DataHelper.*;
import static org.assertj.core.api.Assertions.assertThat;

public class TableDescriptorTest {

    private static final FakeDatasetService fakeDatasetService = new FakeDatasetService();
    private static final FakeJobService fakeJobService = new FakeJobService();
    private static final FakeBigQueryServices fakeBigQueryServices = new FakeBigQueryServices()
        .withDatasetService(fakeDatasetService)
        .withJobService(fakeJobService);

    private static BigQueryConfig bigQueryConfig;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @BeforeAll
    static void setup() throws IOException, InterruptedException {
        FakeDatasetService.setUp();

        fakeDatasetService.createDataset(PROJECT_ID, DATASET_STREAM_MYSQL, REGION, "", 0L);
        fakeDatasetService.createDataset(PROJECT_ID, DATASET_STREAM_POSTGRESQL, REGION, "", 0L);
        fakeDatasetService.createDataset(PROJECT_ID, DATASET_STREAM_MONGODB, REGION, "", 0L);

        fakeDatasetService.createTable(MySqlTable.STREAM_ORDERS);
        fakeDatasetService.createTable(PostgreSqlTable.STREAM_USERS);
        fakeDatasetService.createTable(MongoDbTable.STREAM_TRANSACTIONS);

        bigQueryConfig = new BigQueryConfig();
        bigQueryConfig.setProjectId(PROJECT_ID);
        bigQueryConfig.setRegion(REGION);
        bigQueryConfig.setDatasetId("bronze");
        bigQueryConfig.setCreateDisposition("CREATE_IF_NEEDED");
        bigQueryConfig.setPartitionExpiryMillis(172800000L);
        bigQueryConfig.setTemporaryGcsBucket(FileSystems.getDefault().getPath("").toAbsolutePath() + "/build/tmp");

        DateTimeUtils.setCurrentMillisFixed(WRITE_TIME);
    }

    @AfterAll
    static void tearDown() throws IOException, InterruptedException {
        fakeDatasetService.deleteDataset(PROJECT_ID, DATASET_STREAM_MYSQL);
        fakeDatasetService.deleteDataset(PROJECT_ID, DATASET_STREAM_POSTGRESQL);
        fakeDatasetService.deleteDataset(PROJECT_ID, DATASET_STREAM_MONGODB);
    }

    @Test
    void testTableDescriptor_withValidRows_thenExpectValidResults() throws IOException, InterruptedException {
        Map<String, Table> tablesByName = Stream.of(
                "src/test/resources/schema/mysqlstaging/github_staging/orders/schema.json",
                "src/test/resources/schema/postgresqlstaging/github_staging/users/schema.json",
                "src/test/resources/schema/mongodbstaging/github_staging/transactions/schema.json")
            .map(SchemaLoader::loadTableSchema)
            .collect(Collectors.toMap(Table::getName, Function.identity()));

        pipeline
            .apply("ValidPayload", Create.of(
                    InputMySql.KAFKA_CREATE_OP,
                    InputMySql.KAFKA_UPDATE_OP,
                    InputMySql.KAFKA_DELETE_OP,
                    InputPostgreSql.KAFKA_CREATE_OP,
                    InputPostgreSql.KAFKA_UPDATE_OP,
                    InputPostgreSql.KAFKA_DELETE_OP,
                    InputMongoDb.KAFKA_CREATE_OP,
                    InputMongoDb.KAFKA_UPDATE_OP_WITH_PATCH_AND_SET,
                    InputMongoDb.KAFKA_UPDATE_OP_WITH_PATCH_ONLY,
                    InputMongoDb.KAFKA_UPDATE_OP_WITH_DIFF_I,
                    InputMongoDb.KAFKA_UPDATE_OP_WITH_DIFF_U,
                    InputMongoDb.KAFKA_UPDATE_OP_WITH_DIFF_U_AND_I,
                    InputMongoDb.KAFKA_DELETE_OP)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertToDataPoolStreamRecord", ParDo.of(DataPoolStream.transform(tablesByName)))
            .apply("AddPartitionColumn", ParDo.of(Partitioner.partition(tablesByName, bigQueryConfig)))
            .apply("WriteRecordsToBigQuery", BigQueryIO.writeTableRows()
                .to(TableDescriptor.describe(bigQueryConfig))
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withTestServices(fakeBigQueryServices)
                .withCustomGcsTempLocation(StaticValueProvider.of(bigQueryConfig.getTemporaryGcsBucket())));

        pipeline.run();

        // Assert MySQL rows
        List<String> actualMySqlRows = fakeDatasetService
            .getAllRows(PROJECT_ID, DATASET_STREAM_MYSQL, "github_staging_orders")
            .stream()
            .map(TableRow::toString)
            .collect(Collectors.toList());
        assertThat(actualMySqlRows).containsAnyOf(
            OutputMySql.PARTITIONED_TABLE_ROW_CREATE_OP.toString(),
            OutputMySql.PARTITIONED_TABLE_ROW_UPDATE_OP.toString(),
            OutputMySql.PARTITIONED_TABLE_ROW_DELETE_OP.toString()
        );

        // Assert PostgreSQL rows
        List<String> actualPostgreSqlRows = fakeDatasetService
            .getAllRows(PROJECT_ID, DATASET_STREAM_POSTGRESQL, "github_staging_users")
            .stream()
            .map(TableRow::toString)
            .collect(Collectors.toList());
        assertThat(actualPostgreSqlRows).containsAnyOf(
            OutputPostgreSql.PARTITIONED_TABLE_ROW_CREATE_OP.toString(),
            OutputPostgreSql.PARTITIONED_TABLE_ROW_UPDATE_OP.toString(),
            OutputPostgreSql.PARTITIONED_TABLE_ROW_DELETE_OP.toString()
        );

        // Assert MongoDB rows
        List<String> actualMongoDbRows = fakeDatasetService
            .getAllRows(PROJECT_ID, DATASET_STREAM_MONGODB, "github_staging_transactions")
            .stream()
            .map(TableRow::toString)
            .collect(Collectors.toList());
        assertThat(actualMongoDbRows).containsAnyOf(
            OutputMongoDb.PARTITIONED_TABLE_ROW_CREATE_OP.toString(),
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_PATCH_AND_SET.toString(),
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_PATCH_ONLY.toString(),
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_DIFF_I.toString(),
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_DIFF_U.toString(),
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_DIFF_U_AND_I.toString(),
            OutputMongoDb.PARTITIONED_TABLE_ROW_DELETE_OP.toString()
        );
    }
}
