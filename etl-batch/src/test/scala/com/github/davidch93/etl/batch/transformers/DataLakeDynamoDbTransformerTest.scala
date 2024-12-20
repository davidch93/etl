package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.core.schema.SchemaLoader
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class DataLakeDynamoDbTransformerTest extends AnyFunSuite with Matchers {

  test("Loading Kafka DynamoDB data should return a valid Data Lake") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val kafkaTopic = "dynamodbstaging.github_staging.transactions"
    val kafkaBootstrapServers = "localhost:9092"
    val startScheduledTimestamp = LocalDateTime.parse("2024-12-19T17:00:00")
    val endScheduledTimestamp = LocalDateTime.parse("2024-12-20T17:00:00")
    val dataLakePath = "build/tmp"

    val schemaFilePath = "src/test/resources/schema/dynamodb/github_staging_transactions/schema.json"
    val tableSchema = SchemaLoader.loadTableSchema(schemaFilePath)

    val jsonFile = "src/test/resources/kafka/dynamodb/github_staging_transactions.json"
    val testingDataFrame = spark.read.option("multiline", "true").json(jsonFile)

    val (rawLakeDataFrame, curatedLakeDataFrame) = Kafka
      .read(kafkaTopic)
      .withKafkaBootstrapServers(kafkaBootstrapServers)
      .withTimeRange(startScheduledTimestamp, endScheduledTimestamp)
      .withTable(tableSchema)
      .withDataLakePath(dataLakePath)
      .loadAndWriteDataLake(isTesting = true, Some(testingDataFrame))

    val expectedRawLakeDataFrame = Array(
      Row(1733813344632L, "c", null, "{\"id\":2,\"user_id\":2,\"order_id\":2,\"notes\":null,\"created_at\":1733813319170,\"updated_at\":1733813319170}", "{\"version\":\"1.0.0\",\"connector\":\"DYNAMODB\",\"ts_ms\":\"1733813319670\",\"name\":\"dynamodbstaging\",\"db\":\"github_staging\",\"table\":\"transactions\",\"shard_id\":\"shardId-00000001696411483678-f3df68e4\",\"sequence_number\":\"600000000019036501898\"}", "2024-12-10T13:00:00"),
      Row(1733813345632L, "u", "{\"id\":2,\"user_id\":2,\"order_id\":2,\"notes\":\"\",\"created_at\":1733813319170,\"updated_at\":1733813319170}", "{\"id\":2,\"user_id\":2,\"order_id\":2,\"notes\":\"example\",\"created_at\":1733813319170,\"updated_at\":1733813320170}", "{\"version\":\"1.0.0\",\"connector\":\"DYNAMODB\",\"ts_ms\":\"1733813320670\",\"name\":\"dynamodbstaging\",\"db\":\"github_staging\",\"table\":\"transactions\",\"shard_id\":\"shardId-00000001696411483678-f3df68e4\",\"sequence_number\":\"600000000019036501899\"}", "2024-12-10T13:00:00"),
      Row(1733813346632L, "d", "{\"id\":2,\"user_id\":2,\"order_id\":2,\"notes\":\"example\",\"created_at\":1733813319170,\"updated_at\":1733813320170}", null, "{\"version\":\"1.0.0\",\"connector\":\"DYNAMODB\",\"ts_ms\":\"1733813321670\",\"name\":\"dynamodbstaging\",\"db\":\"github_staging\",\"table\":\"transactions\",\"shard_id\":\"shardId-00000001696411483678-f3df68e4\",\"sequence_number\":\"600000000019036501900\"}", "2024-12-10T13:00:00")
    )

    rawLakeDataFrame.collect() should contain theSameElementsAs expectedRawLakeDataFrame

    val expectedCuratedLakeDataFrame = Array(
      Row(2L, 2L, 2L, null, 1733813319170L, 1733813319170L, false, 1733813319670L, "shardId-00000001696411483678-f3df68e4", "600000000019036501898"),
      Row(2L, 2L, 2L, "example", 1733813319170L, 1733813320170L, false, 1733813320670L, "shardId-00000001696411483678-f3df68e4", "600000000019036501899"),
      Row(2L, 2L, 2L, "example", 1733813319170L, 1733813320170L, true, 1733813321670L, "shardId-00000001696411483678-f3df68e4", "600000000019036501900"),
    )

    curatedLakeDataFrame.collect() should contain theSameElementsAs expectedCuratedLakeDataFrame
  }
}
