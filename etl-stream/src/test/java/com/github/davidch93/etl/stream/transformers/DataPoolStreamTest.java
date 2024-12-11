package com.github.davidch93.etl.stream.transformers;

import com.github.davidch93.etl.core.schema.SchemaLoader;
import com.github.davidch93.etl.core.schema.Table;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeUtils;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.davidch93.etl.stream.DataHelper.*;

public class DataPoolStreamTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @BeforeAll
    static void setup() {
        DateTimeUtils.setCurrentMillisFixed(WRITE_TIME);
    }

    @Test
    void testTransform_withValidPayload_thenExpectValidResults() {
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
            .apply("ConvertToDataPoolStreamRecord", ParDo.of(DataPoolStream.transform(tablesByName)));

        PAssert.that(actualPCollections).containsInAnyOrder(
            OutputMySql.TABLE_ROW_CREATE_OP,
            OutputMySql.TABLE_ROW_UPDATE_OP,
            OutputMySql.TABLE_ROW_DELETE_OP,
            OutputPostgreSql.TABLE_ROW_CREATE_OP,
            OutputPostgreSql.TABLE_ROW_UPDATE_OP,
            OutputPostgreSql.TABLE_ROW_DELETE_OP,
            OutputMongoDb.TABLE_ROW_CREATE_OP,
            OutputMongoDb.TABLE_ROW_UPDATE_OP_WITH_PATCH_AND_SET,
            OutputMongoDb.TABLE_ROW_UPDATE_OP_WITH_PATCH_ONLY,
            OutputMongoDb.TABLE_ROW_UPDATE_OP_WITH_DIFF_I,
            OutputMongoDb.TABLE_ROW_UPDATE_OP_WITH_DIFF_U,
            OutputMongoDb.TABLE_ROW_UPDATE_OP_WITH_DIFF_U_AND_I,
            OutputMongoDb.TABLE_ROW_DELETE_OP
        );

        pipeline.run();
    }

    @Test
    void testTransform_withUnsupportedSourceType_thenExpectDataIsNotProcessed() {
        String schemaFilePath = "src/test/resources/schema/dynamodbstaging/github_staging/transactions/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);
        Map<String, Table> tablesByName = Map.of(table.getName(), table);

        PCollection<TableRow> actualPCollections = pipeline
            .apply("InvalidSourceType", Create
                .of(InputDynamoDb.KAFKA_CREATE_OP)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertToDataPoolStreamRecord", ParDo.of(DataPoolStream.transform(tablesByName)));

        PAssert.that(actualPCollections).empty();

        pipeline.run();
    }
}
