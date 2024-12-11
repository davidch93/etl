package com.github.davidch93.etl.stream.transformers;

import com.github.davidch93.etl.core.config.BigQueryConfig;
import com.github.davidch93.etl.core.schema.SchemaLoader;
import com.github.davidch93.etl.core.schema.Table;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeUtils;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.davidch93.etl.stream.DataHelper.*;

public class PartitionerTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static BigQueryConfig bigQueryConfig;

    @BeforeAll
    static void setup() {
        bigQueryConfig = new BigQueryConfig();
        bigQueryConfig.setProjectId(PROJECT_ID);
        bigQueryConfig.setRegion(REGION);
        bigQueryConfig.setPartitionExpiryMillis(172800000L);

        DateTimeUtils.setCurrentMillisFixed(WRITE_TIME);
    }

    @Test
    void testPartition_withValidPayload_thenExpectValidResults() {
        Map<String, Table> tablesByName = Stream.of(
                "src/test/resources/schema/mysqlstaging/github_staging/orders/schema.json",
                "src/test/resources/schema/postgresqlstaging/github_staging/users/schema.json",
                "src/test/resources/schema/mongodbstaging/github_staging/transactions/schema.json")
            .map(SchemaLoader::loadTableSchema)
            .collect(Collectors.toMap(Table::getName, Function.identity()));

        PCollection<TableRow> actualPCollections = pipeline
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
            .apply("AddPartitionColumn", ParDo.of(Partitioner.partition(tablesByName, bigQueryConfig)));

        PAssert.that(actualPCollections).containsInAnyOrder(
            OutputMySql.PARTITIONED_TABLE_ROW_CREATE_OP,
            OutputMySql.PARTITIONED_TABLE_ROW_UPDATE_OP,
            OutputMySql.PARTITIONED_TABLE_ROW_DELETE_OP,
            OutputPostgreSql.PARTITIONED_TABLE_ROW_CREATE_OP,
            OutputPostgreSql.PARTITIONED_TABLE_ROW_UPDATE_OP,
            OutputPostgreSql.PARTITIONED_TABLE_ROW_DELETE_OP,
            OutputMongoDb.PARTITIONED_TABLE_ROW_CREATE_OP,
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_PATCH_AND_SET,
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_PATCH_ONLY,
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_DIFF_I,
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_DIFF_U,
            OutputMongoDb.PARTITIONED_TABLE_ROW_UPDATE_OP_WITH_DIFF_U_AND_I,
            OutputMongoDb.PARTITIONED_TABLE_ROW_DELETE_OP
        );

        pipeline.run();
    }

    @Test
    void testPartition_withExpiredPayload_thenExpectDataIsNotProcessed() {
        String schemaFilePath = "src/test/resources/schema/mysqlstaging/github_staging/orders/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);
        Map<String, Table> tablesByName = Map.of(table.getName(), table);

        PCollection<TableRow> actualPCollections = pipeline
            .apply("ExpiredPayload", Create
                .of(InputMySql.KAFKA_EXPIRED_PAYLOAD)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertToDataPoolStreamRecord", ParDo.of(DataPoolStream.transform(tablesByName)))
            .apply("AddPartitionColumn", ParDo.of(Partitioner.partition(tablesByName, bigQueryConfig)));

        PAssert.that(actualPCollections).empty();

        pipeline.run();
    }

    @Test
    void testPartition_whenErrorParsing_thenExpectDataIsNotProcessed() {
        String schemaFilePath = "src/test/resources/schema/mysqlstaging/github_staging/orders/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);
        Map<String, Table> tablesByName = Map.of(table.getName(), table);

        PCollection<TableRow> actualPCollections = pipeline
            .apply("ValidPayload", Create
                .of(InputMySql.KAFKA_CREATE_OP)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertToDataPoolStreamRecord", ParDo.of(DataPoolStream.transform(tablesByName)))
            .apply("ModifyField", ParDo.of(new DoFn<TableRow, TableRow>() {

                @ProcessElement
                public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
                    // Modify the `created_at` value to simulate an error on the `Partitioner` transformation
                    TableRow modifiedRow = row.clone();
                    modifiedRow.set("created_at", "Not an epoch milliseconds");

                    out.output(modifiedRow);
                }
            }))
            .apply("AddPartitionColumn", ParDo.of(Partitioner.partition(tablesByName, bigQueryConfig)));

        PAssert.that(actualPCollections).empty();

        pipeline.run();
    }
}
