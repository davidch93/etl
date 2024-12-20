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

class DataPoolPostgreSqlTransformerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder().master("local[1]").getOrCreate()

  val schemaFilePath = "src/test/resources/schema/postgresql/github_staging_users/schema.json"
  val table: Table = SchemaLoader.loadTableSchema(schemaFilePath)

  val dataPoolPath = "build/tmp/test/postgresql/github_staging_users/snapshot=2024-12-09"

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("WARN")

    val tablePartitionOpt = Option(table.getTablePartition.orElse(null))
    val schemaStruct = SchemaConverter.tableSchemaWithMetadataToStructType(table.getSchema, tablePartitionOpt)

    val rows = Seq(
      Row(1L, "data-user@example.com", 1733813310170L, 1733813310170L, false)
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

  test("Loading and deduplicating PostgreSQL data pool should return a valid DataPool") {
    val kafkaTopic = "postgresqlstaging.github_staging.users"
    val kafkaBootstrapServers = "localhost:9092"
    val startScheduledTimestamp = LocalDateTime.parse("2024-12-09T17:00:00")
    val endScheduledTimestamp = LocalDateTime.parse("2024-12-10T17:00:00")
    val dataLakePath = "build/tmp"

    val jsonFile = "src/test/resources/kafka/postgresql/github_staging_users.json"
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
      Row(1L, "data-user@example.com", 1733813310170L, 1733813310170L, false),
      Row(2L, null, null, null, true)
    )

    poolDataFrame.collect() should contain theSameElementsAs expectedPoolDataFrame
  }
}
