package com.github.davidch93.etl.batch.cli

import com.beust.jcommander.JCommander
import com.github.davidch93.etl.batch.cli.Command._

/**
 * The `BatchCLI` class provides a command-line interface (CLI) for executing ETL pipelines.
 * It supports commands for creating data pool for the scheduled time, allowing users to
 * interact with the ETL Batch system.
 *
 * <h3>Supported Commands:</h3>
 *  - `create_data_pool`: Starts the data pool pipeline.
 *
 * This class uses JCommander to parse command-line arguments and map them to their respective
 * commands. It also orchestrates the execution of the corresponding ETL pipelines.
 *
 * <h3>Key Responsibilities:</h3>
 *  1. Parse CLI arguments to determine the selected command and its parameters.
 *  1. Instantiate and configure the appropriate pipeline (`DataPoolPipeline`, etc.).
 *  1. Handle help requests and invalid input gracefully.
 *
 * <h3>Example Usage:</h3>
 * <pre>
 * val args = Array("create_data_pool", "--config-path", "gs://bucket/config.json", "--scheduled-timestamp", "2024-12-16T10:00:00Z")
 * new BatchCLI().parse(args).run()
 * </pre>
 *
 * @author david.christianto
 */
class BatchCLI {

  private var dataPoolCommand = new DataPoolCommand()
  private val jCommander: JCommander = JCommander
    .newBuilder()
    .addCommand(dataPoolCommand)
    .build()

  /**
   * Replaces the default `DataPoolCommand` with a custom instance.
   * Useful for dependency injection or testing.
   *
   * @param dataPoolCommand A custom `DataPoolCommand` instance.
   * @return The current `BatchCLI` instance for method chaining.
   */
  def applyCommand(dataPoolCommand: DataPoolCommand): BatchCLI = {
    this.dataPoolCommand = dataPoolCommand
    this
  }

  /**
   * Parses the command-line arguments and determines the command to execute.
   *
   * @param args An array of command-line arguments.
   * @return The current `BatchCLI` instance for method chaining.
   * @throws com.beust.jcommander.ParameterException If the arguments are invalid or incomplete.
   */
  def parse(args: Array[String]): BatchCLI = {
    jCommander.parse(args.toArray: _*)
    this
  }

  /**
   * Executes the selected command and its corresponding ETL pipeline.
   *
   *  - If no command is specified, throws an exception.
   *  - If help is requested for a command, displays usage information.
   *  - Otherwise, prepares and starts the selected pipeline.
   *
   * @throws Exception If no valid command is specified.
   */
  def run(): Unit = {
    if (jCommander.getParsedCommand == null) {
      throw new Exception("Please specify a command to be executed!")
    }

    val pipeline = jCommander.getParsedCommand match {
      case DATA_POOL_CMD =>
        if (dataPoolCommand.help) {
          jCommander.getCommands.get(DATA_POOL_CMD).usage()
          return
        }
        dataPoolCommand.createPipeline()
    }

    pipeline.prepareAndStart()
  }
}
