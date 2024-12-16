package com.github.davidch93.etl.sentry.cli

import com.beust.jcommander.{Parameter, Parameters}
import com.github.davidch93.etl.core.pipelines.EtlPipeline
import com.github.davidch93.etl.sentry.pipelines.ValidationPipeline
import org.slf4j.{Logger, LoggerFactory}

/**
 * The `ValidationCommand` class represents the command-line configuration for starting the
 * data validation pipeline. It uses JCommander for parameter parsing.
 *
 * This command validates data in BigQuery tables based on predefined rules and generates
 * validation reports. It requires two parameters:
 *  - `--config-path`: The GCS path to the configuration file.
 *  - `--scheduled-timestamp`: The timestamp of the scheduled execution.
 *
 * Once configured, the `createPipeline` method instantiates a `ValidationPipeline` for execution.
 *
 * @author david.christianto
 */
@Parameters(
  commandNames = Array(Command.VALIDATION_CMD),
  commandDescription = "Start the data pool validation pipeline")
class ValidationCommand {

  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  @Parameter(
    names = Array("--config-path"),
    description = "The configuration path in GCS",
    order = 1,
    required = true)
  private[cli] var configPath: String = _

  @Parameter(
    names = Array("--scheduled-timestamp"),
    description = "The timestamp for the scheduled execution of the validation pipeline.",
    order = 2,
    required = true)
  private[cli] var scheduledTimestamp: String = _

  @Parameter(names = Array("--help"), help = true)
  private[cli] var help: Boolean = _

  /**
   * Creates an instance of the `ValidationPipeline` with the provided parameters.
   *
   * @return An instance of `EtlPipeline` configured for data validation.
   */
  def createPipeline(): EtlPipeline = {
    logger.info(
      f"""[ETL-SENTRY] Start the validation pipeline with params:
         |--config-path $configPath
         |--start-scheduled-timestamp $scheduledTimestamp""".stripMargin)
    new ValidationPipeline(configPath, scheduledTimestamp)
  }
}
