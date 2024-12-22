package com.github.davidch93.etl.batch.cli

import com.beust.jcommander.{Parameter, Parameters}
import com.github.davidch93.etl.batch.pipelines.DataPoolPipeline
import com.github.davidch93.etl.core.pipelines.EtlPipeline
import org.slf4j.{Logger, LoggerFactory}

/**
 * The `DataPoolCommand` class represents the command-line configuration for starting the
 * data pool pipeline. It uses JCommander for parameter parsing.
 *
 * It requires two parameters:
 *  - `--config-path`: The GCS path to the configuration file.
 *  - `--scheduled-timestamp`: The timestamp of the scheduled execution.
 *
 * Once configured, the `createPipeline` method instantiates a `DataPoolPipeline` for execution.
 *
 * @author david.christianto
 */
@Parameters(
  commandNames = Array(Command.DATA_POOL_CMD),
  commandDescription = "Start the data pool pipeline")
class DataPoolCommand {

  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  @Parameter(
    names = Array("--config-path"),
    description = "The configuration path in GCS",
    order = 1,
    required = true)
  private[cli] var configPath: String = _

  @Parameter(
    names = Array("--scheduled-timestamp"),
    description = "The timestamp for the scheduled execution of the data pool pipeline.",
    order = 2,
    required = true)
  private[cli] var scheduledTimestamp: String = _

  @Parameter(names = Array("--help"), help = true)
  private[cli] var help: Boolean = _

  /**
   * Creates an instance of the `DataPoolPipeline` with the provided parameters.
   *
   * @return An instance of `EtlPipeline` configured for creating data pool.
   */
  def createPipeline(): EtlPipeline = {
    logger.info(
      f"""[ETL-BATCH] Start the data pool pipeline with params:
         |--config-path $configPath
         |--scheduled-timestamp $scheduledTimestamp""".stripMargin)
    new DataPoolPipeline(configPath, scheduledTimestamp)
  }
}
