package com.github.davidch93.etl.sentry.cli

import com.beust.jcommander.{Parameter, Parameters}
import com.github.davidch93.etl.core.pipelines.EtlPipeline
import com.github.davidch93.etl.sentry.pipelines.ConstraintSuggestionPipeline
import org.slf4j.{Logger, LoggerFactory}

/**
 * The `ConstraintSuggestionCommand` class represents the command-line configuration for
 * starting the constraint suggestion pipeline. It uses JCommander for parameter parsing.
 *
 * This command analyzes a specified BigQuery table to suggest potential data quality
 * constraints using Amazon Deequ. It requires one parameter:
 *  - `--bigquery-table-name`: The fully qualified name of the BigQuery table to analyze.
 *
 * Once configured, the `createPipeline` method instantiates a `ConstraintSuggestionPipeline` for execution.
 *
 * @author david.christianto
 */
@Parameters(
  commandNames = Array(Command.CONSTRAINT_SUGGESTION_CMD),
  commandDescription = "Start the data pool constraint suggestion pipeline")
class ConstraintSuggestionCommand {

  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  @Parameter(
    names = Array("--bigquery-table-name"),
    description = "The fully qualified name of the BigQuery table",
    order = 1,
    required = true)
  private[cli] var bigQueryTableName: String = _

  @Parameter(names = Array("--help"), help = true)
  private[cli] var help: Boolean = _

  /**
   * Creates an instance of the `ConstraintSuggestionPipeline` with the provided table name.
   *
   * @return An instance of `EtlPipeline` configured for constraint suggestion.
   */
  def createPipeline(): EtlPipeline = {
    logger.info(
      f"""[ETL-SENTRY] Start the constraint suggestion pipeline with params:
         |--bigquery-table-name $bigQueryTableName""".stripMargin)
    new ConstraintSuggestionPipeline(bigQueryTableName)
  }
}
