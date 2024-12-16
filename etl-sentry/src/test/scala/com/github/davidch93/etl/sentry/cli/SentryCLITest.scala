package com.github.davidch93.etl.sentry.cli

import com.beust.jcommander.MissingCommandException
import com.github.davidch93.etl.sentry.pipelines.{ConstraintSuggestionPipeline, ValidationPipeline}
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SentryCLITest extends AnyFunSuite with Matchers with MockitoSugar {

  test("Running without specifying command should produce an Exception") {
    val args = Array.empty[String]
    val interceptedException = intercept[Exception] {
      new SentryCLI().parse(args).run()
    }
    interceptedException.getMessage shouldEqual "Please specify a command to be executed!"
  }

  test("Running with unsupported command should produce an Exception") {
    val args = Array("test")
    val interceptedException = intercept[MissingCommandException] {
      new SentryCLI().parse(args).run()
    }
    assert(interceptedException.getMessage equals "Expected a command, got test")
  }

  test("Running a validation command when help argument is true then should print the usage on the console") {
    val args = Array(Command.VALIDATION_CMD, "--help")
    new SentryCLI().parse(args).run()
  }

  test("Running a validation command should start a validation pipeline") {
    val mockedValidationCommand = mock[ValidationCommand]
    val mockedValidationPipeline = mock[ValidationPipeline]
    when(mockedValidationCommand.createPipeline()).thenReturn(mockedValidationPipeline)
    doNothing.when(mockedValidationPipeline).prepareAndStart()

    val args = Array(
      Command.VALIDATION_CMD,
      "--config-path", "src/test/resources/config/github-etl-sentry-staging.json",
      "--scheduled-timestamp", "2024-12-16T17:00:00",
    )

    new SentryCLI()
      .applyCommand(mockedValidationCommand)
      .parse(args)
      .run()
  }

  test("Running a constraints suggestion command when help argument is true then should print the usage on the console") {
    val args = Array(Command.CONSTRAINT_SUGGESTION_CMD, "--help")
    new SentryCLI().parse(args).run()
  }

  test("Running a constraints suggestion command should start a constraints suggestion pipeline") {
    val mockedConstraintsSuggestionCommand = mock[ConstraintSuggestionCommand]
    val mockedConstraintsSuggestionPipeline = mock[ConstraintSuggestionPipeline]
    when(mockedConstraintsSuggestionCommand.createPipeline()).thenReturn(mockedConstraintsSuggestionPipeline)
    doNothing.when(mockedConstraintsSuggestionPipeline).prepareAndStart()

    val args = Array(
      Command.CONSTRAINT_SUGGESTION_CMD,
      "--bigquery-table-name",
      "bronze_daily_mysql.github_staging_orders"
    )

    new SentryCLI()
      .applyCommand(mockedConstraintsSuggestionCommand)
      .parse(args)
      .run()
  }
}
