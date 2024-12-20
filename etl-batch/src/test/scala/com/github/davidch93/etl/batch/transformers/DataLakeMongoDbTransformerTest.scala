package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.core.schema.SchemaLoader
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class DataLakeMongoDbTransformerTest extends AnyFunSuite with Matchers {

  test("Loading Kafka MongoDB data should return a valid Data Lake") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val kafkaTopic = "mongodbstaging.github_staging.transactions"
    val kafkaBootstrapServers = "localhost:9092"
    val startScheduledTimestamp = LocalDateTime.parse("2024-12-19T17:00:00")
    val endScheduledTimestamp = LocalDateTime.parse("2024-12-20T17:00:00")
    val dataLakePath = "build/tmp"

    val schemaFilePath = "src/test/resources/schema/mongodb/github_staging_transactions/schema.json"
    val tableSchema = SchemaLoader.loadTableSchema(schemaFilePath)

    val jsonFile = "src/test/resources/kafka/mongodb/github_staging_transactions.json"
    val testingDataFrame = spark.read.option("multiline", "true").json(jsonFile)

    val (rawLakeDataFrame, curatedLakeDataFrame) = Kafka
      .read(kafkaTopic)
      .withKafkaBootstrapServers(kafkaBootstrapServers)
      .withTimeRange(startScheduledTimestamp, endScheduledTimestamp)
      .withTable(tableSchema)
      .withDataLakePath(dataLakePath)
      .loadAndWriteDataLake(isTesting = true, Some(testingDataFrame))

    val expectedRawLakeDataFrame = Array(
      Row(1733813344632L, "c", null, "{\"_id\":{\"$oid\":\"630b23db6ef80123d6e72d74\"},\"user_id\":{\"$numberInt\":2},\"order_id\":{\"$numberLong\":2},\"fee\":{\"$numberDecimal\":\"12345.6789\"},\"created_at\":{\"$date\":1733813319170},\"updated_at\":{\"$date\":1733813319170}}", "{\"version\":\"1.0.0\",\"connector\":\"MONGODB\",\"ts_ms\":1733813319670,\"name\":\"mongodbstaging\",\"db\":\"github_staging\",\"collection\":\"transactions\",\"ord\":31}", "2024-12-10T13:00:00"),
      Row(1733813345632L, "u", null, "{\"fee\":{\"$numberDecimal\":\"12345.67891\"},\"updated_at\":{\"$date\":1733813320170},\"_id\":{\"$oid\":\"630b23db6ef80123d6e72d74\"}}", "{\"version\":\"1.0.0\",\"connector\":\"MONGODB\",\"ts_ms\":1733813320670,\"name\":\"mongodbstaging\",\"db\":\"github_staging\",\"collection\":\"transactions\",\"ord\":32}", "2024-12-10T13:00:00"),
      Row(1733813346632L, "u", null, "{\"fee\":{\"$numberDecimal\":\"12345.67892\"},\"updated_at\":{\"$date\":1733813321170},\"_id\":{\"$oid\":\"630b23db6ef80123d6e72d74\"}}", "{\"version\":\"1.0.0\",\"connector\":\"MONGODB\",\"ts_ms\":1733813321670,\"name\":\"mongodbstaging\",\"db\":\"github_staging\",\"collection\":\"transactions\",\"ord\":33}", "2024-12-10T13:00:00"),
      Row(1733813347632L, "u", null, "{\"notes\":\"example\",\"_id\":{\"$oid\":\"630b23db6ef80123d6e72d74\"}}", "{\"version\":\"1.0.0\",\"connector\":\"MONGODB\",\"ts_ms\":1733813322670,\"name\":\"mongodbstaging\",\"db\":\"github_staging\",\"collection\":\"transactions\",\"ord\":34}", "2024-12-10T13:00:00"),
      Row(1733813348632L, "u", null, "{\"updated_at\":{\"$date\":1733813322170},\"_id\":{\"$oid\":\"630b23db6ef80123d6e72d74\"}}", "{\"version\":\"1.0.0\",\"connector\":\"MONGODB\",\"ts_ms\":1733813323670,\"name\":\"mongodbstaging\",\"db\":\"github_staging\",\"collection\":\"transactions\",\"ord\":35}", "2024-12-10T13:00:00"),
      Row(1733813349632L, "u", null, "{\"updated_at\":{\"$date\":1733813323170},\"notes\":{\"user\":\"data\",\"status\":\"PAID\"},\"_id\":{\"$oid\":\"630b23db6ef80123d6e72d74\"}}", "{\"version\":\"1.0.0\",\"connector\":\"MONGODB\",\"ts_ms\":1733813324670,\"name\":\"mongodbstaging\",\"db\":\"github_staging\",\"collection\":\"transactions\",\"ord\":36}", "2024-12-10T13:00:00"),
      Row(1733813350632L, "d", null, "{\"_id\":{\"$oid\":\"630b23db6ef80123d6e72d74\"}}", "{\"version\":\"1.0.0\",\"connector\":\"MONGODB\",\"ts_ms\":1733813325670,\"name\":\"mongodbstaging\",\"db\":\"github_staging\",\"collection\":\"transactions\",\"ord\":37}", "2024-12-10T13:00:00")
    )

    rawLakeDataFrame.collect() should contain theSameElementsAs expectedRawLakeDataFrame

    val expectedCuratedLakeDataFrame = Array(
      Row("{\"$oid\":\"630b23db6ef80123d6e72d74\"}", 2L, 2L, 12345.6789, null, 1733813319170L, 1733813319170L, false, 1733813319670L, 31L),
      Row("{\"$oid\":\"630b23db6ef80123d6e72d74\"}", null, null, 12345.67891, null, null, 1733813320170L, false, 1733813320670L, 32L),
      Row("{\"$oid\":\"630b23db6ef80123d6e72d74\"}", null, null, 12345.67892, null, null, 1733813321170L, false, 1733813321670L, 33L),
      Row("{\"$oid\":\"630b23db6ef80123d6e72d74\"}", null, null, null, "example", null, null, false, 1733813322670L, 34L),
      Row("{\"$oid\":\"630b23db6ef80123d6e72d74\"}", null, null, null, null, null, 1733813322170L, false, 1733813323670L, 35L),
      Row("{\"$oid\":\"630b23db6ef80123d6e72d74\"}", null, null, null, "{\"user\":\"data\",\"status\":\"PAID\"}", null, 1733813323170L, false, 1733813324670L, 36L),
      Row("{\"$oid\":\"630b23db6ef80123d6e72d74\"}", null, null, null, null, null, null, true, 1733813325670L, 37L),
    )

    curatedLakeDataFrame.collect() should contain theSameElementsAs expectedCuratedLakeDataFrame
  }
}
