package com.github.davidch93.etl.batch.helpers

import com.github.davidch93.etl.core.config.BigQueryConfig
import com.github.davidch93.etl.core.constants.Dataset
import com.github.davidch93.etl.core.utils.{EmailUtils, KafkaTopicResolver}
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import java.io.{PrintWriter, StringWriter}
import java.time.Instant
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * A helper class to manage and execute batch pipeline jobs for ETL processes.
 * This includes job execution, error notification, and managing concurrency.
 *
 * @param groupName      The group name of the job, used for logging and notifications.
 * @param maxThreads     The maximum number of concurrent threads allowed for job execution.
 * @param bigQueryConfig The BigQuery configuration used for logging job results and metadata.
 * @author david.christianto
 */
class PipelineHelper private(groupName: String, maxThreads: Int, bigQueryConfig: BigQueryConfig) {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val executorService = Executors.newFixedThreadPool(maxThreads)
  private val executionContext = ExecutionContext.fromExecutorService(executorService)

  /**
   * Executes a series of pipeline jobs concurrently.
   *
   * @param tableWhitelist An array of table names to process.
   * @param runJob         A function to start an ETL job for a given table.
   * @param finalizeJob    A function to perform post-execution operations on a `Row` representing job metadata.
   * @return `true` if all jobs are successful, `false` if any fail.
   */
  def executeJobs(tableWhitelist: Array[String], runJob: String => Unit, finalizeJob: Row => Unit): Boolean = {
    val allJobsSuccessful = tableWhitelist
      .map(kafkaTopic =>
        Future {
          submitJob(kafkaTopic, runJob, finalizeJob)
        }(executionContext))
      .map(future => Await.result(future, Duration.Inf))
      .forall(status => status)

    logger.info(s"[ETL-BATCH] Shutting down the executor service for the `$groupName` job!")
    executorService.shutdown()
    logger.info(s"[ETL-BATCH] Finished shutting down the executor service for the `$groupName` job!")

    allJobsSuccessful
  }

  /**
   * Sends an email notification for any failed jobs.
   *
   * @param failedJobRows      An array of `Row` objects representing metadata for failed jobs.
   * @param scheduledTimestamp The timestamp when the job execution was scheduled.
   */
  def notifyOnFailedJobs(failedJobRows: Array[Row], scheduledTimestamp: String): Unit = {
    val data = failedJobRows.mkString("\n")
    val message =
      s"""
         |There are failed jobs for $groupName ETL dag in $scheduledTimestamp:
         |$data
         |
         |--------------------------
         |""".stripMargin

    val subject = s"[P3][Failed Data Pool Jobs - $groupName - $scheduledTimestamp]"
    EmailUtils.sendEmail(subject, message)
  }

  /**
   * Executes a single pipeline job and logs its metadata.
   *
   * @param kafkaTopic  The Kafka topic or job identifier for the job.
   * @param runJob      A function to start an ETL job.
   * @param finalizeJob A function to perform post-execution operations on a `Row` representing job metadata.
   * @return `true` if the job is successful, `false` if any fails.
   */
  private def submitJob(kafkaTopic: String, runJob: String => Unit, finalizeJob: Row => Unit): Boolean = {
    val startEpochSeconds = Instant.now().getEpochSecond

    val (isSuccess, message) = try {
      runJob(kafkaTopic)
      (true, "")
    } catch {
      case ex: Exception => (false, extractStackTrace(ex))
    }

    val endEpochSeconds = Instant.now().getEpochSecond
    val durationSeconds = endEpochSeconds - startEpochSeconds

    val resolver = KafkaTopicResolver.resolve(kafkaTopic)
    val datasetId = bigQueryConfig.getDatasetId(Dataset.DAILY, resolver.identifySource())
    val bigQueryTableName = s"$datasetId.${resolver.getDatabaseName}_${resolver.getTableName}"

    finalizeJob(
      Row(
        bigQueryTableName,
        startEpochSeconds,
        endEpochSeconds,
        durationSeconds,
        isSuccess,
        message
      )
    )

    isSuccess
  }

  /**
   * Extracts the stack trace from a throwable as a string.
   *
   * @param throwable The exception or error.
   * @return The stack trace as a string.
   */
  private def extractStackTrace(throwable: Throwable): String = {
    val stringWriter = new StringWriter()
    throwable.printStackTrace(new PrintWriter(stringWriter))
    stringWriter.toString
  }
}

/**
 * Companion object for `PipelineHelper` to provide factory methods.
 *
 * @author david.christianto
 */
object PipelineHelper {

  /**
   * Creates a new `PipelineHelper` instance.
   *
   * @param groupName      The group name of the job.
   * @param maxThreads     The maximum number of concurrent threads for job execution.
   * @param bigQueryConfig The BigQuery configuration.
   * @return A `PipelineHelper` instance.
   */
  def apply(groupName: String, maxThreads: Int, bigQueryConfig: BigQueryConfig): PipelineHelper = {
    new PipelineHelper(groupName, maxThreads, bigQueryConfig)
  }
}
