package com.github.davidch93.etl.stream.pipelines;

import com.github.davidch93.etl.core.pipelines.EtlPipeline;
import com.github.davidch93.etl.core.schema.Table;
import com.github.davidch93.etl.stream.config.StreamConfigLoader;
import com.github.davidch93.etl.stream.config.StreamConfiguration;
import com.github.davidch93.etl.stream.helper.BigQueryHelper;
import com.github.davidch93.etl.stream.helper.SchemaHelper;
import com.github.davidch93.etl.stream.options.StreamOptions;
import com.github.davidch93.etl.stream.transformers.DataPoolStream;
import com.github.davidch93.etl.stream.transformers.Partitioner;
import com.github.davidch93.etl.stream.transformers.TableDescriptor;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A pipeline responsible for streaming data from Kafka to BigQuery in real-time.
 * <p>
 * This pipeline reads data from Kafka topics, transforms it according to the provided schemas,
 * partitions the data, and writes it to BigQuery tables.
 * </p>
 * <p>
 * This pipeline is configured using {@link StreamOptions} and {@link StreamConfiguration}.
 * </p>
 * <p><strong>Note:</strong> Instances of this class are intended to be used with Apache Beam pipelines.</p>
 *
 * @author david.christianto
 */
public class DataPoolStreamPipeline implements EtlPipeline {

    private static final Logger logger = LoggerFactory.getLogger(DataPoolStreamPipeline.class);

    private final Pipeline pipeline;
    private final StreamConfiguration config;
    private final SchemaHelper schemaHelper;
    private final BigQueryHelper bigQueryHelper;

    private Map<String, Table> tablesByName;

    /**
     * Constructs a new {@code DataPoolStreamPipeline} instance with the provided stream options.
     *
     * @param streamOptions The options for configuring the data stream.
     */
    public DataPoolStreamPipeline(StreamOptions streamOptions) {
        this.pipeline = Pipeline.create(streamOptions);
        this.config = StreamConfigLoader.load().fromConfigPath(streamOptions.getConfigPath());
        this.schemaHelper = new SchemaHelper(config.getConfigBucket());
        this.bigQueryHelper = new BigQueryHelper(streamOptions, config.getBigQueryConfig());
    }

    @Override
    public void prepare() {
        logger.info("[ETL-STREAM] Loading schemas from `{}`.", config.getConfigBucket());
        tablesByName = config.getTableWhitelist().stream()
            .map(schemaHelper::loadTableSchema)
            .collect(Collectors.toMap(Table::getName, Function.identity()));

        logger.info("[DATAFLOW] Preparing all BigQuery stream tables and real-time views if required.");
        bigQueryHelper.prepareStreamTablesAndRealTimeViews(tablesByName.values());
    }

    @Override
    public void start() {
        pipeline
            .apply("ReadKafkaEvents", KafkaIO.<String, String>read()
                .withBootstrapServers(config.getKafkaConfig().getBootstrapServers())
                .withTopics(config.getTableWhitelist())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(ImmutableMap.of(
                    ConsumerConfig.GROUP_ID_CONFIG, config.getKafkaConfig().getGroupId(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getKafkaConfig().getDefaultOffset()))
                .withProcessingTime()
                .commitOffsetsInFinalize())
            .apply("ConvertToDataPoolStreamRecord", ParDo.of(DataPoolStream.transform(tablesByName)))
            .apply("AddPartitionColumn", ParDo.of(Partitioner.partition(tablesByName, config.getBigQueryConfig())))
            .apply("WriteRecordsToBigQuery", BigQueryIO.writeTableRows()
                .to(TableDescriptor.describe(config.getBigQueryConfig()))
                .withoutValidation()
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withMethod(Method.STREAMING_INSERTS));

        logger.info("[ETL-STREAM] Starting the data pool stream pipeline.");
        pipeline.run();
    }
}
