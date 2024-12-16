package com.github.davidch93.etl.sentry.pipelines

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.repository.ResultKey
import com.github.davidch93.etl.core.pipelines.EtlPipeline
import com.github.davidch93.etl.core.schema.SchemaLoader
import com.github.davidch93.etl.core.utils.{BigQueryTableResolver, DateTimeUtils}
import com.github.davidch93.etl.sentry.config.SentryConfigLoader
import com.github.davidch93.etl.sentry.constants.Field._
import com.github.davidch93.etl.sentry.repository.BigQueryMetricsRepository
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._

/**
 * The `ValidationPipeline` class orchestrates data validation tasks within the ETL process.
 *
 * This pipeline performs the following:
 *  - Prepares execution details such as timestamp and date.
 *  - Validates BigQuery tables based on predefined schemas and rules.
 *  - Executes validation jobs for tables and saves results in a metrics repository.
 *  - Sends notifications when data quality issues are detected.
 *
 * @param configPath         The path to the configuration file.
 * @param scheduledTimestamp The timestamp for the scheduled execution.
 * @author david.christianto
 */
class ValidationPipeline(configPath: String, scheduledTimestamp: String) extends EtlPipeline {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val config = SentryConfigLoader.fromConfigPath(configPath)
  private val spark = SparkSession.builder()
    .appName("DataValidationPipeline")
    .getOrCreate()

  private val pipelineHelper = PipelineHelper(config.maxThreads)
  private val metricsRepository = BigQueryMetricsRepository(spark, config.deequRepositoryConfig)

  private var executionDate: String = _
  private var executionTimestampMillis: Long = _

  /**
   * Prepares the pipeline by calculating the execution date and timestamp in milliseconds.
   */
  override def prepare(): Unit = {
    val localDateTime = DateTimeUtils.parseIsoLocalDateTime(scheduledTimestamp)

    executionDate = localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    executionTimestampMillis = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli

    logger.info(s"[ETL-SENTRY] The current execution date: $executionDate")
    logger.info(s"[ETL-SENTRY] The current execution timestamp in milliseconds: $executionTimestampMillis")
  }

  /**
   * Executes the validation pipeline by processing and validating tables.
   * Sends notifications for any data quality issues.
   */
  override def start(): Unit = {
    logger.info(s"[ETL-SENTRY] Starting ${config.groupName} jobs.")

    val tablesToValidate = filterTablesToValidate(config.tableWhitelist)
    val isValidationSuccessful = pipelineHelper.executeJobs(tablesToValidate, validateTable)

    if (!isValidationSuccessful) {
      doNotifyOnDataQualityIssueDetected()
    }

    logger.info(f"[ETL-SENTRY] All ${config.groupName} jobs finished!")
  }

  /**
   * Filters tables that require validation by checking previous validation results.
   *
   *  - If no summary history is found, it will run all the given items.
   *  - If the summary history is found, it will filter out all successful items in the summary history.
   *
   * @param tableWhitelist The list of tables to validate.
   * @return A list of tables that require validation.
   */
  private def filterTablesToValidate(tableWhitelist: Array[String]): Array[String] = {
    val validTables = metricsRepository
      .loadSummaryTable(s"DATE($TIMESTAMP) >= '$executionDate' " +
        s"AND $VALIDATION_DATE = $executionTimestampMillis " +
        s"AND $GROUP_NAME = '${config.groupName}' " +
        s"AND $IS_VALID = true")
      .select(TABLE_NAME)
      .collect()
      .map(_.getAs[String](TABLE_NAME))

    tableWhitelist.filter(!validTables.contains(_))
  }

  /**
   * Validates a BigQuery table and saves the validation results.
   *
   * @param bigQueryTableName The name of the BigQuery table to validate.
   * @return `true` if the validation is successful; otherwise, `false`.
   */
  private def validateTable(bigQueryTableName: String): Boolean = {
    val resolver = BigQueryTableResolver.resolve(bigQueryTableName)
    val schemaFilePath = s"schema/${resolver.identifySource.toString.toLowerCase}/${resolver.getTableName}/schema.json"
    val tableSchema = SchemaLoader.loadTableSchema(schemaFilePath)

    val fullyQualifiedTableName = s"${config.sourceBigQueryConfig.getProjectId}.$bigQueryTableName"
    val dataFrame = spark
      .read
      .format("bigquery")
      .load(fullyQualifiedTableName)

    val resultKey = ResultKey(
      executionTimestampMillis,
      Map(
        SOURCE -> tableSchema.getSource.toString,
        TABLE_NAME -> bigQueryTableName,
        GROUP_NAME -> config.groupName
      )
    )

    val verificationResult = VerificationSuite()
      .onData(dataFrame)
      .addChecks(pipelineHelper.generateChecks(tableSchema.getSchema.getFields.asScala.toList))
      .useRepository(metricsRepository)
      .saveOrAppendResult(resultKey)
      .run()

    metricsRepository.saveValidationSummary(resultKey, verificationResult)

    verificationResult.status == CheckStatus.Success
  }

  /**
   * Sends notifications when data quality issues are detected.
   */
  private def doNotifyOnDataQualityIssueDetected(): Unit = {
    val invalidRows = metricsRepository
      .loadSummaryTable(s"DATE($TIMESTAMP) >= '$executionDate' " +
        s"AND $VALIDATION_DATE = $executionTimestampMillis " +
        s"AND $GROUP_NAME = '${config.groupName}' " +
        s"AND $IS_VALID = false")
      .select(TABLE_NAME, MESSAGE)
      .collect()

    pipelineHelper.notifyOnInvalidData(invalidRows, config.groupName, scheduledTimestamp)
  }
}
