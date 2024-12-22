package com.github.davidch93.etl.sentry.helpers

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.github.davidch93.etl.core.schema.FieldRule.Rule._
import com.github.davidch93.etl.core.schema.{Field, FieldRule}
import com.github.davidch93.etl.core.utils.EmailUtils
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/**
 * Utility class providing helper methods for data quality pipelines.
 *
 * This class facilitates parallel job execution, data quality rule checks,
 * and sending notifications for data quality issues. It leverages Spark,
 * Deequ checks, and email notifications to streamline pipeline operations.
 *
 * @constructor Private constructor to initialize a pipeline helper with the specified thread pool size.
 * @param groupName  The group name of the job.
 * @param maxThreads The maximum number of threads to use for parallel job execution.
 * @author david.christianto
 */
class PipelineHelper private(groupName: String, maxThreads: Int) {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val executorService = Executors.newFixedThreadPool(maxThreads)
  private val executionContext = ExecutionContext.fromExecutorService(executorService)

  /**
   * Executes jobs in parallel for the provided list of table names.
   *
   * @param tableWhitelist Array of table names to process.
   * @param validateTable  Function to validate a single table. Returns `true` if the job succeeds, `false` otherwise.
   * @return `true` if all jobs succeed, `false` if any fail.
   */
  def executeJobs(tableWhitelist: Array[String], validateTable: String => Boolean): Boolean = {
    val allJobsSuccessful = tableWhitelist
      .map(table =>
        Future {
          validateTable(table)
        }(executionContext))
      .map(future => Await.result(future, Duration.Inf))
      .forall(status => status)

    logger.info(s"[ETL-SENTRY] Shutting down the executor service for the `$groupName` job!")
    executorService.shutdown()
    logger.info(s"[ETL-SENTRY] Finished shutting down the executor service for the `$groupName` job!")

    allJobsSuccessful
  }

  /**
   * Generates Deequ checks for the provided list of fields based on their rules.
   *
   * @param fields List of `Field` objects containing field definitions and associated rules.
   * @return Sequence of `Check` objects representing data quality constraints.
   */
  def generateChecks(fields: List[Field]): Seq[Check] = {
    fields.flatMap { field =>
      val fieldName = field.getName
      Option(field.getRules)
        .map(_.asScala)
        .getOrElse(Seq.empty)
        .map { fieldRule =>
          createCheckForRule(fieldName, fieldRule)
        }
    }
  }

  /**
   * Creates a Deequ `Check` for a specific field rule.
   *
   * @param fieldName The name of the field to which the rule applies.
   * @param fieldRule The `FieldRule` defining the data quality constraint.
   * @return A `Check` object representing the data quality rule.
   * @throws IllegalArgumentException If the rule is unsupported.
   */
  private def createCheckForRule(fieldName: String, fieldRule: FieldRule): Check = {
    val rule = fieldRule.getRule
    val hint = Option(fieldRule.getHint).getOrElse("-")

    rule match {
      case HAS_COMPLETENESS =>
        val assertionValue = fieldRule.getAssertion
        Check(CheckLevel.Error, rule.toString).hasCompleteness(fieldName, _ >= assertionValue, hint = Some(hint))
      case IS_COMPLETE =>
        Check(CheckLevel.Error, rule.toString).isComplete(fieldName, hint = Some(hint))
      case IS_CONTAINED_IN =>
        val allowedValues = fieldRule.getValues.asScala.toArray
        Check(CheckLevel.Error, rule.toString).isContainedIn(fieldName, allowedValues, hint = Some(hint))
      case IS_NON_NEGATIVE =>
        Check(CheckLevel.Error, rule.toString).isNonNegative(fieldName, hint = Some(hint))
      case IS_PRIMARY_KEY =>
        Check(CheckLevel.Error, rule.toString).isPrimaryKey(fieldName, hint = Some(hint))
      case IS_UNIQUE =>
        Check(CheckLevel.Error, rule.toString).isUnique(fieldName, hint = Some(hint))
      case _ =>
        throw new IllegalArgumentException(s"Unsupported rule for `$rule`!")
    }
  }

  /**
   * Sends an email notification for invalid data detected during validation.
   *
   * @param invalidRows        Array of invalid rows causing data quality issues.
   * @param scheduledTimestamp The scheduled timestamp for the validation run.
   */
  def notifyOnInvalidData(invalidRows: Array[Row], scheduledTimestamp: String): Unit = {
    val invalidDataDetails = invalidRows.mkString("\n")
    val message =
      s"""
         |There are data quality problems for $groupName DAG in $scheduledTimestamp:\n
         |$invalidDataDetails
         |
         |--------------------------
         |""".stripMargin

    val subject = s"[P3][Data quality issue was detected in the $groupName group on $scheduledTimestamp]"
    EmailUtils.sendEmail(subject, message)
  }
}

/**
 * Companion object for creating instances of `PipelineHelper`.
 *
 * @author david.christianto
 */
object PipelineHelper {

  /**
   * Creates a new `PipelineHelper` instance with the specified maximum number of threads.
   *
   * @param groupName  The group name of the job.
   * @param maxThreads The maximum number of threads to use for parallel processing.
   * @return A `PipelineHelper` instance.
   */
  def apply(groupName: String, maxThreads: Int): PipelineHelper = {
    new PipelineHelper(groupName, maxThreads)
  }
}
