package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.core.schema.SchemaLoader
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class DataLakePostgreSqlTransformerTest extends AnyFunSuite with Matchers {

  test("Loading Kafka PostgreSQL data should return a valid Data Lake") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val kafkaTopic = "postgresqlstaging.github_staging.users"
    val kafkaBootstrapServers = "localhost:9092"
    val startScheduledTimestamp = LocalDateTime.parse("2024-12-19T17:00:00")
    val endScheduledTimestamp = LocalDateTime.parse("2024-12-20T17:00:00")
    val dataLakePath = "build/tmp"

    val schemaFilePath = "src/test/resources/schema/postgresql/github_staging_users/schema.json"
    val tableSchema = SchemaLoader.loadTableSchema(schemaFilePath)

    val jsonFile = "src/test/resources/kafka/postgresql/github_staging_users.json"
    val testingDataFrame = spark.read.option("multiline", "true").json(jsonFile)

    val (rawLakeDataFrame, curatedLakeDataFrame) = Kafka
      .read(kafkaTopic)
      .withKafkaBootstrapServers(kafkaBootstrapServers)
      .withTimeRange(startScheduledTimestamp, endScheduledTimestamp)
      .withTable(tableSchema)
      .withDataLakePath(dataLakePath)
      .loadAndWriteDataLake(isTesting = true, Some(testingDataFrame))

    val expectedRawLakeDataFrame = Array(
      Row(1733813344632L, "c", null, "{\"id\":2,\"email\":\"user@example.com\",\"created_at\":1733813319170,\"updated_at\":1733813319170}", "{\"version\":\"1.0.0\",\"connector\":\"POSTGRESQL\",\"ts_ms\":1733813319670,\"name\":\"postgresqlstaging\",\"db\":\"github_staging\",\"schema\":\"public\",\"table\":\"users\",\"lsn\":24023128,\"txid\":728530626}", "2024-12-10T13:00:00"),
      Row(1733813345632L, "u", null, "{\"id\":2,\"email\":\"user-updated@example.com\",\"created_at\":1733813319170,\"updated_at\":1733813320170}", "{\"version\":\"1.0.0\",\"connector\":\"POSTGRESQL\",\"ts_ms\":1733813320670,\"name\":\"postgresqlstaging\",\"db\":\"github_staging\",\"schema\":\"public\",\"table\":\"users\",\"lsn\":24023129,\"txid\":728530627}", "2024-12-10T13:00:00"),
      Row(1733813346632L, "d", null, "{\"id\":2}", "{\"version\":\"1.0.0\",\"connector\":\"POSTGRESQL\",\"ts_ms\":1733813321670,\"name\":\"postgresqlstaging\",\"db\":\"github_staging\",\"schema\":\"public\",\"table\":\"users\",\"lsn\":24023130,\"txid\":728530628}", "2024-12-10T13:00:00")
    )

    rawLakeDataFrame.collect() should contain theSameElementsAs expectedRawLakeDataFrame

    val expectedCuratedLakeDataFrame = Array(
      Row(2L, "user@example.com", 1733813319170L, 1733813319170L, false, 1733813319670L, 24023128L),
      Row(2L, "user-updated@example.com", 1733813319170L, 1733813320170L, false, 1733813320670L, 24023129L),
      Row(2L, null, null, null, true, 1733813321670L, 24023130L),
    )

    curatedLakeDataFrame.collect() should contain theSameElementsAs expectedCuratedLakeDataFrame
  }
}
