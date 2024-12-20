package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.batch.helpers.SchemaConverter
import com.github.davidch93.etl.core.schema.{SchemaLoader, Table}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class DataPoolMongoDbTransformerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder().master("local[1]").getOrCreate()

  val schemaFilePath = "src/test/resources/schema/mongodb/github_staging_transactions/schema.json"
  val table: Table = SchemaLoader.loadTableSchema(schemaFilePath)

  val dataPoolPath = "build/tmp/test/mongodb/github_staging_transactions/snapshot=2024-12-09"

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("WARN")

    val tablePartitionOpt = Option(table.getTablePartition.orElse(null))
    val schemaStruct = SchemaConverter.tableSchemaWithMetadataToStructType(table.getSchema, tablePartitionOpt)

    val rows = Seq(
      Row("{\"$oid\":\"630b23db6ef80123d6e72d70\"}", 1L, 1L, 12345.6789, null, 1733813310170L, 1733813310170L, false)
    )

    // Create Data Pool to Filesystem
    val poolDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rows), schemaStruct)
    poolDataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(dataPoolPath)
  }

  override def afterAll(): Unit = {
    val path = new Path(dataPoolPath)
    val fileSystem = path.getFileSystem(new Configuration())

    // Delete Data Pool from Filesystem
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }

  test("Loading and deduplicating MongoDB data pool should return a valid DataPool") {
    val kafkaTopic = "mongodbstaging.github_staging.transactions"
    val kafkaBootstrapServers = "localhost:9092"
    val startScheduledTimestamp = LocalDateTime.parse("2024-12-09T17:00:00")
    val endScheduledTimestamp = LocalDateTime.parse("2024-12-10T17:00:00")
    val dataLakePath = "build/tmp"

    val jsonFile = "src/test/resources/kafka/mongodb/github_staging_transactions.json"
    val testingDataFrame = spark.read.option("multiline", "true").json(jsonFile)

    val (_, curatedLakeDataFrame) = Kafka
      .read(kafkaTopic)
      .withKafkaBootstrapServers(kafkaBootstrapServers)
      .withTimeRange(startScheduledTimestamp, endScheduledTimestamp)
      .withTable(table)
      .withDataLakePath(dataLakePath)
      .loadAndWriteDataLake(isTesting = true, Some(testingDataFrame))

    val poolDataFrame = DataPool
      .forPath(dataPoolPath)
      .mergeWith(curatedLakeDataFrame)
      .withTable(table)
      .load()

    val expectedPoolDataFrame = Array(
      Row("{\"$oid\":\"630b23db6ef80123d6e72d70\"}", 1L, 1L, 12345.6789, null, 1733813310170L, 1733813310170L, false),
      Row("{\"$oid\":\"630b23db6ef80123d6e72d74\"}", 2L, 2L, 12345.67892, "{\"user\":\"data\",\"status\":\"PAID\"}", 1733813319170L, 1733813323170L, true)
    )

    poolDataFrame.collect() should contain theSameElementsAs expectedPoolDataFrame
  }
}
