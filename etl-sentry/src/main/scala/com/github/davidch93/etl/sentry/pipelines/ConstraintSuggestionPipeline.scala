package com.github.davidch93.etl.sentry.pipelines

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.github.davidch93.etl.core.pipelines.EtlPipeline
import com.github.davidch93.etl.core.utils.EmailUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * The `ConstraintSuggestionPipeline` class is responsible for generating constraint suggestions for a given
 * BigQuery table using Amazon Deequ. These suggestions help in defining data quality rules by analyzing
 * the table's structure and content.
 *
 * The pipeline performs the following steps:
 *  - Loads the specified BigQuery table into a Spark DataFrame.
 *  - Runs Amazon Deequ's `ConstraintSuggestionRunner` to identify possible constraints.
 *  - Generates a summary of suggested constraints and their respective Scala code for implementation.
 *  - Sends an email with the suggestions, providing insights into the data quality rules that can be applied.
 *
 * @param bigQueryTableName The fully qualified name of the BigQuery table to analyze.
 * @author david.christianto
 */
class ConstraintSuggestionPipeline(bigQueryTableName: String) extends EtlPipeline {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val spark = SparkSession.builder()
    .appName("ConstraintsSuggestionPipeline")
    .getOrCreate()

  /**
   * Prepares the pipeline for execution.
   */
  override def prepare(): Unit = {
  }

  /**
   * Executes the pipeline by analyzing the BigQuery table and sending the generated constraint suggestions.
   */
  override def start(): Unit = {
    logger.info("[ETL-SENTRY] Starting the constraints suggestion pipeline.")

    val dataFrame = spark
      .read
      .format("bigquery")
      .load(bigQueryTableName)

    val suggestionResult = ConstraintSuggestionRunner()
      .onData(dataFrame)
      .addConstraintRules(Rules.DEFAULT)
      .run()

    val checkSuggestions = suggestionResult.constraintSuggestions.flatMap { case (column, suggestions) =>
      suggestions.map { suggestion =>
        s"""
           |- Constraint suggestion for `$column`: ${suggestion.description}
           |  The corresponding Scala code is ${suggestion.codeForConstraint}
           |""".stripMargin
      }
    }.mkString("")

    val subject = s"[Data Quality] Constraint Check suggestions for the `$bigQueryTableName` table"
    val message =
      s"""
         |The constraint check suggestions for the `$bigQueryTableName` are the following:
         |Please take note that you can define appropriate rules for your table.
         |$checkSuggestions
         |--------------------------
         |""".stripMargin

    EmailUtils.sendEmail(subject, message)
  }
}
