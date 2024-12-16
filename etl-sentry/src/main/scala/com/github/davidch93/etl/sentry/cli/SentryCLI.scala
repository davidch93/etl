package com.github.davidch93.etl.sentry.cli

import com.beust.jcommander.JCommander
import com.github.davidch93.etl.sentry.cli.Command._

/**
 * The `SentryCLI` class provides a command-line interface (CLI) for executing ETL pipelines.
 * It supports commands for data validation and constraint suggestion, allowing users to
 * interact with the ETL Sentry system.
 *
 * <h3>Supported Commands:</h3>
 *  - `validate`: Starts the data validation pipeline.
 *  - `suggest_constraint_rules`: Starts the constraint suggestion pipeline.
 *
 * This class uses JCommander to parse command-line arguments and map them to their respective
 * commands. It also orchestrates the execution of the corresponding ETL pipelines.
 *
 * <h3>Key Responsibilities:</h3>
 *  1. Parse CLI arguments to determine the selected command and its parameters.
 *  1. Instantiate and configure the appropriate pipeline (`ValidationPipeline` or
 *     `ConstraintSuggestionPipeline`).
 *  1. Handle help requests and invalid input gracefully.
 *
 * <h3>Example Usage:</h3>
 * <pre>
 * val args = Array("validate", "--config-path", "gs://bucket/config.json", "--scheduled-timestamp", "2024-12-16T10:00:00Z")
 * new SentryCLI().parse(args).run()
 * </pre>
 */
class SentryCLI {

  private var constraintSuggestionCommand = new ConstraintSuggestionCommand()
  private var validationCommand = new ValidationCommand()
  private val jCommander: JCommander = JCommander
    .newBuilder()
    .addCommand(constraintSuggestionCommand)
    .addCommand(validationCommand)
    .build()

  /**
   * Replaces the default `ConstraintSuggestionCommand` with a custom instance.
   * Useful for dependency injection or testing.
   *
   * @param constraintSuggestionCommand A custom `ConstraintSuggestionCommand` instance.
   * @return The current `SentryCLI` instance for method chaining.
   */
  def applyCommand(constraintSuggestionCommand: ConstraintSuggestionCommand): SentryCLI = {
    this.constraintSuggestionCommand = constraintSuggestionCommand
    this
  }

  /**
   * Replaces the default `ValidationCommand` with a custom instance.
   * Useful for dependency injection or testing.
   *
   * @param validationCommand A custom `ValidationCommand` instance.
   * @return The current `SentryCLI` instance for method chaining.
   */
  def applyCommand(validationCommand: ValidationCommand): SentryCLI = {
    this.validationCommand = validationCommand
    this
  }

  /**
   * Parses the command-line arguments and determines the command to execute.
   *
   * @param args An array of command-line arguments.
   * @return The current `SentryCLI` instance for method chaining.
   * @throws com.beust.jcommander.ParameterException If the arguments are invalid or incomplete.
   */
  def parse(args: Array[String]): SentryCLI = {
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
      case CONSTRAINT_SUGGESTION_CMD =>
        if (constraintSuggestionCommand.help) {
          jCommander.getCommands.get(CONSTRAINT_SUGGESTION_CMD).usage()
          return
        }
        constraintSuggestionCommand.createPipeline()
      case VALIDATION_CMD =>
        if (validationCommand.help) {
          jCommander.getCommands.get(VALIDATION_CMD).usage()
          return
        }
        validationCommand.createPipeline()
    }

    pipeline.prepareAndStart()
  }
}
