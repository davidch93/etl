package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.batch.helpers.SchemaConverter
import com.github.davidch93.etl.batch.writers.GcsWriter
import com.github.davidch93.etl.core.constants.MetadataField._
import com.github.davidch93.etl.core.constants.Source
import com.github.davidch93.etl.core.schema.Table
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.{LocalDateTime, ZoneOffset}

/**
 * A transformer class for reading data from Kafka, transforming it, and
 * writing it to a Data Lake in raw and curated formats.
 *
 * Scala example to load Kafka data with a specific time range:
 * {{{
 *   Kafka
 *     .read(kafkaTopic)
 *     .withKafkaBootstrapServers(kafkaBootstrapServers)
 *     .withTimeRange(startTimestamp, endTimestamp)
 *     .withTable(tableSchema)
 *     .withDataLakePath(dataLakePath)
 *     .loadAndWriteDataLake()
 * }}}
 *
 * @param spark                 The active `SparkSession`.
 * @param kafkaTopic            The Kafka topic to subscribe to.
 * @param kafkaBootstrapServers The Kafka bootstrap servers for reading messages.
 * @param startTimestamp        The start timestamp for consuming messages from Kafka.
 * @param endTimestamp          The end timestamp for consuming messages from Kafka.
 * @param table                 The table metadata, including partitioning information.
 * @param dataLakePath          The target path in the Data Lake for storing the output.
 * @author david.christianto
 */
class Kafka private(
  spark: SparkSession,
  kafkaTopic: String,
  kafkaBootstrapServers: String,
  startTimestamp: LocalDateTime,
  endTimestamp: LocalDateTime,
  table: Table,
  dataLakePath: String,
) {

  import spark.implicits._

  private val transformToRawDataLake: DataFrame => DataFrame = kafkaDataFrame => {
    table.getSource match {
      case Source.MYSQL => DataLakeMySqlTransformer.transformToRawDataLake(kafkaDataFrame)
      case Source.POSTGRESQL => DataLakePostgreSqlTransformer.transformToRawDataLake(kafkaDataFrame)
      case Source.MONGODB => DataLakeMongoDbTransformer.transformToRawDataLake(kafkaDataFrame)
      case Source.DYNAMODB => DataLakeDynamoDbTransformer.transformToRawDataLake(kafkaDataFrame)
    }
  }

  private val transformToCuratedDataLake: DataFrame => DataFrame = rawLakeDataFrame => {
    val schema = SchemaConverter.tableSchemaToStructType(table.getSchema)
    table.getSource match {
      case Source.MYSQL => DataLakeMySqlTransformer.transformToCuratedDataLake(rawLakeDataFrame, schema)
      case Source.POSTGRESQL => DataLakePostgreSqlTransformer.transformToCuratedDataLake(rawLakeDataFrame, schema)
      case Source.MONGODB => DataLakeMongoDbTransformer.transformToCuratedDataLake(rawLakeDataFrame, schema)
      case Source.DYNAMODB => DataLakeDynamoDbTransformer.transformToCuratedDataLake(rawLakeDataFrame, schema)
    }
  }

  /**
   * Loads data from Kafka, applies transformations, and writes the raw and curated data to the Data Lake.
   *
   * Returns a tuple containing the raw and curated DataFrames.
   *
   * The first `DataFrame` is a cached raw data lake with the following schema
   * {{{
   *   root
   *    |-- ts_ms: long (nullable = false)
   *    |-- op: string (nullable = false)
   *    |-- before: string (nullable = true)
   *    |-- after: string (nullable = true)
   *    |-- source: string (nullable = false)
   *    |-- _datetime_partition: string (nullable = false)
   * }}}
   *
   * The second `Dataframe` is a curated data lake with the following schema.
   * {{{
   *   root
   *    |-- id: long (nullable = true)
   *    |-- ...                                           # The other fields
   *    |-- order_timestamp: timestamp (nullable = true)  # A partition field if any
   *    |-- _is_deleted: boolean (nullable = false)
   *    |-- _audit_write_time: long (nullable = false)
   *    |-- _ts_ms: long (nullable = false)
   *    |-- _file: string (nullable = false)
   *    |-- _pos: long (nullable = false)
   * }}}
   *
   * @param isTesting If `true`, skips the write operation and uses the provided test `DataFrame`.
   * @param testingDF An optional `DataFrame` for testing purposes.
   * @return A tuple containing the raw and curated DataFrames.
   */
  private def loadAndWriteDataLake(isTesting: Boolean, testingDF: Option[DataFrame]): (DataFrame, DataFrame) = {
    val rawLakeDataFrame = testingDF.getOrElse(readFromKafka())
      .transform(transformToRawDataLake)
      .repartition($"$DATE_TIME_PARTITION")
      .cache()

    if (!isTesting) {
      GcsWriter.writeAppend(rawLakeDataFrame)
        .withGcsPath(dataLakePath)
        .withPartitions(Array(DATE_TIME_PARTITION))
        .execute()
    }

    val curatedLakeDataFrame = transformToCuratedDataLake(rawLakeDataFrame)
      .withColumn(IS_DELETED, when($"$PAYLOAD_OP" === PAYLOAD_OP_D, true).otherwise(false))
      .transform(addPartitionColumn)

    (rawLakeDataFrame, curatedLakeDataFrame)
  }

  /**
   * Reads data from Kafka as a `DataFrame` based on the configured settings.
   *
   * Connects to Kafka using the specified bootstrap servers and topic. It reads messages
   * based on the provided starting and ending timestamps and casts the key and value to strings.
   *
   * The `startingOffsetsByTimestampStrategy` option will be used when the specified starting offset
   * by timestamp (either global or per partition) doesn't match with the offset Kafka returned.
   *  - "error": fail the query and end users have to deal with workarounds requiring manual steps.
   *  - "latest": assigns the latest offset for these partitions, so that Spark can read newer records
   *    from these partitions in further micro-batches.
   *
   * @return A `DataFrame` containing messages read from Kafka.
   */
  private def readFromKafka(): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingTimestamp", startTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli)
      .option("endingTimestamp", endTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli)
      .option("startingOffsetsByTimestampStrategy", "latest")
      .load()
      .select($"$KEY".cast(StringType), $"$VALUE".cast(StringType))
  }

  /**
   * Adds a partition column to the `DataFrame` based on the table's partitioning configuration.
   *
   * @param curatedLakeDataFrame The curated data lake `DataFrame`.
   * @return The `DataFrame` with the added partition column if any.
   */
  private def addPartitionColumn(curatedLakeDataFrame: DataFrame): DataFrame = {
    val tablePartitionOpt = Option(table.getTablePartition.orElse(null))

    tablePartitionOpt match {
      case Some(tablePartition) =>
        val sourceColumn = tablePartition.getSourceColumn
        val partitionColumn = tablePartition.getPartitionColumn
        val timestampColumn = to_timestamp(from_unixtime($"$STRUCT_DATA.$sourceColumn" / 1000))

        curatedLakeDataFrame
          .withColumn(partitionColumn, timestampColumn)
          .select(
            $"$STRUCT_DATA.*",
            $"$partitionColumn",
            $"$IS_DELETED",
            $"$STRUCT_SOURCE.*"
          )

      case None =>
        curatedLakeDataFrame
          .select(
            $"$STRUCT_DATA.*",
            $"$IS_DELETED",
            $"$STRUCT_SOURCE.*",
          )
    }
  }
}

/**
 * Companion object for creating and configuring instances of the `Kafka` class.
 *
 * @author david.christianto
 */
object Kafka {

  /**
   * Initializes a Kafka builder using the active `SparkSession`.
   *
   * @param kafkaTopic The Kafka topic to read from.
   * @return A `KafkaBuilder` instance for further configuration.
   */
  def read(kafkaTopic: String): KafkaBuilder = {
    val spark = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    read(spark, kafkaTopic)
  }

  /**
   * Initializes a Kafka builder with the specified `SparkSession` and topic.
   *
   * @param spark      The `SparkSession` to use.
   * @param kafkaTopic The Kafka topic to read from.
   * @return A `KafkaBuilder` instance for further configuration.
   */
  def read(spark: SparkSession, kafkaTopic: String): KafkaBuilder = {
    KafkaBuilder(spark, kafkaTopic)
  }

  /**
   * A builder class for configuring Kafka-based ETL operations.
   *
   * @param spark                 The `SparkSession` instance.
   * @param kafkaTopic            The Kafka topic to read from.
   * @param kafkaBootstrapServers The Kafka bootstrap servers.
   * @param startTimestamp        The start timestamp for consuming messages.
   * @param endTimestamp          The end timestamp for consuming messages.
   * @param table                 The table metadata.
   * @param dataLakePath          The Data Lake path for storing the output.
   */
  case class KafkaBuilder private(
    private val spark: SparkSession,
    private val kafkaTopic: String,
    private val kafkaBootstrapServers: String = null,
    private val startTimestamp: LocalDateTime = null,
    private val endTimestamp: LocalDateTime = null,
    private val table: Table = null,
    private val dataLakePath: String = null
  ) {

    /**
     * Specifies the Kafka bootstrap servers to use for connecting to Kafka.
     *
     * @param kafkaBootstrapServers The Kafka bootstrap servers.
     * @return An updated `KafkaBuilder` instance.
     */
    def withKafkaBootstrapServers(kafkaBootstrapServers: String): KafkaBuilder =
      copy(kafkaBootstrapServers = kafkaBootstrapServers)

    /**
     * Specifies the time range for consuming messages from Kafka.
     *
     * @param startTimestamp The start timestamp for consuming messages.
     * @param endTimestamp   The end timestamp for consuming messages.
     * @return An updated `KafkaBuilder` instance.
     */
    def withTimeRange(startTimestamp: LocalDateTime, endTimestamp: LocalDateTime): KafkaBuilder =
      copy(startTimestamp = startTimestamp).copy(endTimestamp = endTimestamp)

    /**
     * Specifies the table metadata, including partitioning information.
     *
     * @param table The table metadata.
     * @return An updated `KafkaBuilder` instance.
     */
    def withTable(table: Table): KafkaBuilder =
      copy(table = table)

    /**
     * Specifies the Data Lake path for storing the output data.
     *
     * @param dataLakePath The Data Lake path.
     * @return An updated `KafkaBuilder` instance.
     */
    def withDataLakePath(dataLakePath: String): KafkaBuilder =
      copy(dataLakePath = dataLakePath)

    /**
     * Executes the ETL process by loading data from Kafka, transforming it, and writing it to the Data Lake.
     *
     * @param isTesting Optional boolean flag indicating whether to run in a testing environment. Defaults to `false`.
     *                  If `true`, skips the write operation and uses the provided test `DataFrame`.
     * @param testingDF Optional `DataFrame` used for testing when `isTesting` is `true`.
     *                  Provides mock data for testing purposes.
     * @return A tuple containing the raw and curated DataFrames.
     */
    def loadAndWriteDataLake(
      isTesting: Boolean = false,
      testingDF: Option[DataFrame] = None
    ): (DataFrame, DataFrame) =
      new Kafka(
        spark,
        kafkaTopic,
        kafkaBootstrapServers,
        startTimestamp,
        endTimestamp,
        table,
        dataLakePath
      ).loadAndWriteDataLake(isTesting, testingDF)
  }
}