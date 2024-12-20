package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.core.schema.SchemaLoader
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

class DataLakeMySqlTransformerTest extends AnyFunSuite with Matchers {

  test("Loading Kafka MySQL data with a partition column should return a valid Data Lake") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val kafkaTopic = "mysqlstaging.github_staging.orders"
    val kafkaBootstrapServers = "localhost:9092"
    val startScheduledTimestamp = LocalDateTime.parse("2024-12-19T17:00:00")
    val endScheduledTimestamp = LocalDateTime.parse("2024-12-20T17:00:00")
    val dataLakePath = "build/tmp"

    val schemaFilePath = "src/test/resources/schema/mysql/github_staging_orders/schema.json"
    val tableSchema = SchemaLoader.loadTableSchema(schemaFilePath)

    val jsonFile = "src/test/resources/kafka/mysql/github_staging_orders.json"
    val testingDataFrame = spark.read.option("multiline", "true").json(jsonFile)

    val (rawLakeDataFrame, curatedLakeDataFrame) = Kafka
      .read(kafkaTopic)
      .withKafkaBootstrapServers(kafkaBootstrapServers)
      .withTimeRange(startScheduledTimestamp, endScheduledTimestamp)
      .withTable(tableSchema)
      .withDataLakePath(dataLakePath)
      .loadAndWriteDataLake(isTesting = true, Some(testingDataFrame))

    val expectedRawLakeDataFrame = Array(
      Row(1733813344632L, "c", null, "{\"id\":2,\"amount\":150000,\"status\":\"PAID\",\"is_expired\":false,\"created_at\":1733813319170}", "{\"version\":\"1.0.0\",\"connector\":\"MYSQL\",\"ts_ms\":1733813319670,\"name\":\"mysqlstaging\",\"db\":\"github_staging\",\"table\":\"orders\",\"server_id\":20241210,\"gtid\":\"50791c3f-66ae-ee15-4e7a-70668ea778de:14981727984\",\"file\":\"binlog-0001\",\"pos\":4,\"xid\":728530626}", "2024-12-10T13:00:00"),
      Row(1733813345632L, "u", "{\"id\":2,\"amount\":150000,\"status\":\"PAID\",\"is_expired\":false,\"created_at\":1733813319170}", "{\"id\":2,\"amount\":150000,\"status\":\"PAID\",\"is_expired\":true,\"created_at\":1733813319170}", "{\"version\":\"1.0.0\",\"connector\":\"MYSQL\",\"ts_ms\":1733813320670,\"name\":\"mysqlstaging\",\"db\":\"github_staging\",\"table\":\"orders\",\"server_id\":20241210,\"gtid\":\"50791c3f-66ae-ee15-4e7a-70668ea778de:14981727985\",\"file\":\"binlog-0001\",\"pos\":5,\"xid\":728530627}", "2024-12-10T13:00:00"),
      Row(1733813346632L, "d", "{\"id\":2,\"amount\":150000,\"status\":\"PAID\",\"is_expired\":true,\"created_at\":1733813319170}", null, "{\"version\":\"1.0.0\",\"connector\":\"MYSQL\",\"ts_ms\":1733813321670,\"name\":\"mysqlstaging\",\"db\":\"github_staging\",\"table\":\"orders\",\"server_id\":20241210,\"gtid\":\"50791c3f-66ae-ee15-4e7a-70668ea778de:14981727986\",\"file\":\"binlog-0001\",\"pos\":6,\"xid\":728530628}", "2024-12-10T13:00:00")
    )

    rawLakeDataFrame.collect() should contain theSameElementsAs expectedRawLakeDataFrame

    val expectedCuratedLakeDataFrame = Array(
      Row(2L, 150000.0, "PAID", false, 1733813319170L, new Timestamp(1733813319000L), false, 1733813319670L, "binlog-0001", 4L),
      Row(2L, 150000.0, "PAID", true, 1733813319170L, new Timestamp(1733813319000L), false, 1733813320670L, "binlog-0001", 5L),
      Row(2L, 150000.0, "PAID", true, 1733813319170L, new Timestamp(1733813319000L), true, 1733813321670L, "binlog-0001", 6L),
    )

    curatedLakeDataFrame.collect() should contain theSameElementsAs expectedCuratedLakeDataFrame
  }
}
