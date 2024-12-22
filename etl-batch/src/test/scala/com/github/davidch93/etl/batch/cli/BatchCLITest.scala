package com.github.davidch93.etl.batch.cli

import com.beust.jcommander.MissingCommandException
import com.github.davidch93.etl.batch.pipelines.DataPoolPipeline
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BatchCLITest extends AnyFunSuite with Matchers with MockitoSugar {

  test("Running without specifying command should produce an Exception") {
    val args = Array.empty[String]
    val interceptedException = intercept[Exception] {
      new BatchCLI().parse(args).run()
    }
    interceptedException.getMessage shouldEqual "Please specify a command to be executed!"
  }

  test("Running with unsupported command should produce an Exception") {
    val args = Array("test")
    val interceptedException = intercept[MissingCommandException] {
      new BatchCLI().parse(args).run()
    }
    assert(interceptedException.getMessage equals "Expected a command, got test")
  }

  test("Running a data pool command when help argument is true then should print the usage on the console") {
    val args = Array(Command.DATA_POOL_CMD, "--help")
    new BatchCLI().parse(args).run()
  }

  test("Running a data pool command should start a data pool pipeline") {
    val mockedDataPoolCommand = mock[DataPoolCommand]
    val mockedDataPoolPipeline = mock[DataPoolPipeline]
    when(mockedDataPoolCommand.createPipeline()).thenReturn(mockedDataPoolPipeline)
    doNothing.when(mockedDataPoolPipeline).prepareAndStart()

    val args = Array(
      Command.DATA_POOL_CMD,
      "--config-path", "src/test/resources/config/github-etl-batch-staging.json",
      "--scheduled-timestamp", "2024-12-22T17:00:00",
    )

    new BatchCLI()
      .applyCommand(mockedDataPoolCommand)
      .parse(args)
      .run()
  }
}
