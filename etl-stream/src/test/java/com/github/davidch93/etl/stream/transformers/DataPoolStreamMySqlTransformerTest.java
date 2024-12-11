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

import static com.github.davidch93.etl.stream.DataHelper.*;

public class DataPoolStreamMySqlTransformerTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static Map<String, Table> tablesByName;

    @BeforeAll
    static void setup() {
        DateTimeUtils.setCurrentMillisFixed(WRITE_TIME);

        String schemaFilePath = "src/test/resources/schema/mysqlstaging/github_staging/orders/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);
        tablesByName = Map.of(table.getName(), table);
    }

    @Test
    void testTransform_withValidPayload_thenExpectValidResults() {
        PCollection<TableRow> actualPCollections = pipeline
            .apply("ValidMysqlPayload", Create.of(
                    InputMySql.KAFKA_CREATE_OP,
                    InputMySql.KAFKA_UPDATE_OP,
                    InputMySql.KAFKA_DELETE_OP)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertToDataPoolStreamRecord", ParDo.of(DataPoolStream.transform(tablesByName)));

        PAssert.that(actualPCollections).containsInAnyOrder(
            OutputMySql.TABLE_ROW_CREATE_OP,
            OutputMySql.TABLE_ROW_UPDATE_OP,
            OutputMySql.TABLE_ROW_DELETE_OP
        );

        pipeline.run();
    }

    @Test
    void testTransform_withInvalidPayload_thenExpectDataIsNotProcessed() {
        PCollection<TableRow> actualPCollections = pipeline
            .apply("InvalidMySqlPayload", Create
                .of(InputMySql.INVALID_KAFKA_PAYLOAD)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertToDataPoolStreamRecord", ParDo.of(DataPoolStream.transform(tablesByName)));

        PAssert.that(actualPCollections).empty();

        pipeline.run();
    }
}
