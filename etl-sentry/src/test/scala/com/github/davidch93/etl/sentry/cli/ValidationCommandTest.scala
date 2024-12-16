package com.github.davidch93.etl.sentry.cli

import com.beust.jcommander.{JCommander, ParameterException}
import com.github.davidch93.etl.sentry.pipelines.ValidationPipeline
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ValidationCommandTest extends AnyFunSuite with Matchers {

  test("Parsing arguments should have all information about the validation command") {
    val command = new ValidationCommand()
    val jCommander = JCommander
      .newBuilder()
      .addCommand(command)
      .build()

    jCommander.parse(
      Command.VALIDATION_CMD,
      "--config-path", "src/test/resources/config/github-etl-sentry-staging.json",
      "--scheduled-timestamp", "2024-12-16T17:00:00",
    )

    command.configPath shouldEqual "src/test/resources/config/github-etl-sentry-staging.json"
    command.scheduledTimestamp shouldEqual "2024-12-16T17:00:00"
  }

  test("Parsing arguments when a required field doesn't exist should produce an Exception") {
    val command = new ValidationCommand()
    val jCommander = JCommander
      .newBuilder()
      .addCommand(command)
      .build()

    val interceptedException = intercept[ParameterException] {
      jCommander.parse(
        Command.VALIDATION_CMD,
        "--config-path", "src/test/resources/config/github-etl-sentry-staging.json",
      )
    }
    interceptedException.getMessage shouldEqual "The following option is required: [--scheduled-timestamp]"
  }

  test("Creating pipeline with specified parameters should create a ValidationPipeline") {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val command = new ValidationCommand()
    val jCommander: JCommander = JCommander
      .newBuilder()
      .addCommand(command)
      .build()

    jCommander.parse(
      Command.VALIDATION_CMD,
      "--config-path", "src/test/resources/config/github-etl-sentry-staging.json",
      "--scheduled-timestamp", "2024-12-16T17:00:00",
    )

    val validationPipeline = command.createPipeline()

    validationPipeline should not be null
    validationPipeline shouldBe a[ValidationPipeline]
  }
}
