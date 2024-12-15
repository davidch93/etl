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

public class DataPoolStreamMongoDbTransformerTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static Map<String, Table> tablesByName;

    @BeforeAll
    static void setup() {
        DateTimeUtils.setCurrentMillisFixed(WRITE_TIME);

        String schemaFilePath = "src/test/resources/schema/mongodb/github_staging_transactions/schema.json";
        Table table = SchemaLoader.loadTableSchema(schemaFilePath);
        tablesByName = Map.of(table.getName(), table);
    }

    @Test
    void testTransform_withValidPayload_thenExpectValidResults() {
        PCollection<TableRow> actualPCollections = pipeline
            .apply("ValidMongoDbPayload", Create.of(
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
    void testTransform_withInvalidPayload_thenExpectDataIsNotProcessed() {
        PCollection<TableRow> actualPCollections = pipeline
            .apply("InvalidMongoDbPayload", Create
                .of(InputMongoDb.INVALID_KAFKA_PAYLOAD)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertToDataPoolStreamRecord", ParDo.of(DataPoolStream.transform(tablesByName)));

        PAssert.that(actualPCollections).empty();

        pipeline.run();
    }
}
