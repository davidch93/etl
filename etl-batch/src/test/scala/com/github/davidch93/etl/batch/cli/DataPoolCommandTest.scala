package com.github.davidch93.etl.batch.cli

import com.beust.jcommander.{JCommander, ParameterException}
import com.github.davidch93.etl.batch.pipelines.DataPoolPipeline
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataPoolCommandTest extends AnyFunSuite with Matchers {

  test("Parsing arguments should have all information about the data pool command") {
    val command = new DataPoolCommand()
    val jCommander = JCommander
      .newBuilder()
      .addCommand(command)
      .build()

    jCommander.parse(
      Command.DATA_POOL_CMD,
      "--config-path", "src/test/resources/config/github-etl-batch-staging.json",
      "--scheduled-timestamp", "2024-12-22T17:00:00",
    )

    command.configPath shouldEqual "src/test/resources/config/github-etl-batch-staging.json"
    command.scheduledTimestamp shouldEqual "2024-12-22T17:00:00"
  }

  test("Parsing arguments when a required field doesn't exist should produce an Exception") {
    val command = new DataPoolCommand()
    val jCommander = JCommander
      .newBuilder()
      .addCommand(command)
      .build()

    val interceptedException = intercept[ParameterException] {
      jCommander.parse(
        Command.DATA_POOL_CMD,
        "--config-path", "src/test/resources/config/github-etl-batch-staging.json",
      )
    }
    interceptedException.getMessage shouldEqual "The following option is required: [--scheduled-timestamp]"
  }

  test("Creating pipeline with specified parameters should create a DataPoolPipeline") {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val command = new DataPoolCommand()
    val jCommander: JCommander = JCommander
      .newBuilder()
      .addCommand(command)
      .build()

    jCommander.parse(
      Command.DATA_POOL_CMD,
      "--config-path", "src/test/resources/config/github-etl-batch-staging.json",
      "--scheduled-timestamp", "2024-12-12T17:00:00",
    )

    val dataPoolPipeline = command.createPipeline()

    dataPoolPipeline should not be null
    dataPoolPipeline shouldBe a[DataPoolPipeline]
  }
}
