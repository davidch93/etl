package com.github.davidch93.etl.sentry.cli

import com.beust.jcommander.{JCommander, ParameterException}
import com.github.davidch93.etl.sentry.pipelines.ConstraintSuggestionPipeline
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConstraintSuggestionCommandTest extends AnyFunSuite with Matchers {

  test("Parsing arguments should have all information about the constraints suggestion command") {
    val command = new ConstraintSuggestionCommand()
    val jCommander = JCommander
      .newBuilder()
      .addCommand(command)
      .build()

    jCommander.parse(
      Command.CONSTRAINT_SUGGESTION_CMD,
      "--bigquery-table-name",
      "bronze_daily_mysql.github_staging_orders"
    )

    command.bigQueryTableName shouldEqual "bronze_daily_mysql.github_staging_orders"
  }

  test("Parsing arguments when a required field doesn't have a value should produce an Exception") {
    val command = new ConstraintSuggestionCommand()
    val jCommander = JCommander
      .newBuilder()
      .addCommand(command)
      .build()

    val interceptedException = intercept[ParameterException] {
      jCommander.parse(Command.CONSTRAINT_SUGGESTION_CMD, "--bigquery-table-name")
    }
    interceptedException.getMessage shouldEqual "Expected a value after parameter --bigquery-table-name"
  }

  test("Creating pipeline with specified parameters should create a ConstraintsSuggestionPipeline") {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val command = new ConstraintSuggestionCommand()
    val jCommander: JCommander = JCommander
      .newBuilder()
      .addCommand(command)
      .build()

    jCommander.parse(
      Command.CONSTRAINT_SUGGESTION_CMD,
      "--bigquery-table-name",
      "bronze_daily_mysql.github_staging_orders"
    )

    val constraintsSuggestionPipeline = command.createPipeline()

    constraintsSuggestionPipeline should not be null
    constraintsSuggestionPipeline shouldBe a[ConstraintSuggestionPipeline]
  }
}
