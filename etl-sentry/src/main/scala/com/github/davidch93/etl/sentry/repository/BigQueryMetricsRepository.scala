package com.github.davidch93.etl.sentry.repository

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.checks.{Check, CheckResult, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.repository.{MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import com.github.davidch93.etl.core.config.BigQueryConfig
import com.github.davidch93.etl.sentry.constants.Field._
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
 * A custom implementation of Deequ's `MetricsRepository` that uses Google BigQuery for persisting
 * and retrieving data quality metrics and validation summaries.
 *
 * @param spark                 The `SparkSession` instance.
 * @param deequRepositoryConfig Deequ configuration details for BigQuery integration.
 * @author david.christianto
 */
class BigQueryMetricsRepository private(
  spark: SparkSession,
  deequRepositoryConfig: BigQueryConfig
) extends MetricsRepository {

  private final val metricsTableName = s"${deequRepositoryConfig.getProjectId}:${deequRepositoryConfig.getDatasetId}.data_quality_metrics"
  private final val summaryTableName = s"${deequRepositoryConfig.getProjectId}:${deequRepositoryConfig.getDatasetId}.data_quality_summary"

  private final val summarySchema: StructType = new StructType()
    .add(VALIDATION_DATE, LongType, nullable = false)
    .add(SOURCE, StringType, nullable = false)
    .add(TABLE_NAME, StringType, nullable = false)
    .add(GROUP_NAME, StringType, nullable = false)
    .add(IS_VALID, BooleanType, nullable = false)
    .add(MESSAGE, StringType)

  /**
   * Saves the analysis metrics to BigQuery.
   *
   * The metrics table schema is the following.
   * {{{
   *   root
   *    |-- validation_date: long (nullable = false)
   *    |-- source: string (nullable = false)
   *    |-- table_name: string (nullable = false)
   *    |-- entity: string (nullable = false)
   *    |-- instance: string (nullable = false)
   *    |-- name: string (nullable = false)
   *    |-- value: double (nullable = false)
   *    |-- timestamp: timestamp (nullable = false)
   * }}}
   *
   * @param resultKey       Metadata identifying the dataset and its associated tags.
   * @param analyzerContext The analysis results to save.
   */
  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val tableName = resultKey.tags(TABLE_NAME)
    val metricsDF = AnalyzerContext.successMetricsAsDataFrame(spark, analyzerContext)
      .withColumn(VALIDATION_DATE, lit(resultKey.dataSetDate))
      .withColumn(SOURCE, lit(resultKey.tags(SOURCE)))
      .withColumn(TABLE_NAME, lit(tableName))
      .withColumn(TIMESTAMP, current_timestamp())

    val tempGcsPath = s"${deequRepositoryConfig.getTemporaryGcsBucket}/metrics/$tableName"
    saveDataFrameToBigQuery(metricsDF, metricsTableName, tempGcsPath)
  }

  /**
   * Loads the analysis metrics by a specific key. Not implemented.
   *
   * @param resultKey The key to filter the metrics.
   * @return Always returns `None`.
   */
  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = {
    None
  }

  /**
   * Loads multiple analysis metrics. Not implemented.
   *
   * @return Always returns `null`.
   */
  override def load(): MetricsRepositoryMultipleResultsLoader = {
    null
  }

  /**
   * Saves the verification result summary to BigQuery.
   *
   * The summary table schema is the following.
   * {{{
   *   root
   *    |-- validation_date: long (nullable = false)
   *    |-- type: string (nullable = false)
   *    |-- table_name: string (nullable = false)
   *    |-- group_name: string (nullable = false)
   *    |-- is_valid: boolean (nullable = false)
   *    |-- message: string (nullable = true)
   *    |-- timestamp: timestamp (nullable = false)
   * }}}
   *
   * @param resultKey          Metadata identifying the dataset and its associated tags.
   * @param verificationResult The verification results to save.
   */
  def saveValidationSummary(resultKey: ResultKey, verificationResult: VerificationResult): Unit = {
    val validationDate = resultKey.dataSetDate
    val tableType = resultKey.tags(SOURCE)
    val tableName = resultKey.tags(TABLE_NAME)
    val groupName = resultKey.tags(GROUP_NAME)
    val isValid = verificationResult.status == CheckStatus.Success
    val message = if (isValid) "" else generateErrorMessage(verificationResult.checkResults)

    val row = Row(validationDate, tableType, tableName, groupName, isValid, message)
    val validationSummaryDF = spark
      .createDataFrame(spark.sparkContext.parallelize(List(row)), summarySchema)
      .withColumn(TIMESTAMP, current_timestamp())

    val tempGcsPath = s"${deequRepositoryConfig.getTemporaryGcsBucket}/summary/$tableName"
    saveDataFrameToBigQuery(validationSummaryDF, summaryTableName, tempGcsPath)
  }

  /**
   * Loads validation summary data from BigQuery with the specified filter expression.
   *
   * @param filterExpression The filter expression to apply.
   * @return A DataFrame containing the filtered results.
   */
  def loadSummaryTable(filterExpression: String): DataFrame = {
    spark
      .read
      .format("bigquery")
      .option("filter", filterExpression)
      .load(summaryTableName)
  }

  /**
   * Saves the given `DataFrame` to BigQuery with the specified options.
   *
   * @param dataFrame               The `DataFrame` to save.
   * @param fullyQualifiedTableName The fully qualified BigQuery table name.
   * @param temporaryGcsBucketPath  The temporary GCS bucket path for staging.
   */
  private def saveDataFrameToBigQuery(
    dataFrame: DataFrame,
    fullyQualifiedTableName: String,
    temporaryGcsBucketPath: String
  ): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("bigquery")
      .option("createDisposition", deequRepositoryConfig.getCreateDisposition)
      .option("temporaryGcsBucket", temporaryGcsBucketPath)
      .option("partitionField", TIMESTAMP)
      .save(fullyQualifiedTableName)
  }

  /**
   * Constructs an error message based on the constraint results of the verification.
   *
   * @param checkResults The results of the checks performed during verification.
   * @return A string representing the error messages for failed constraints.
   */
  private def generateErrorMessage(checkResults: Map[Check, CheckResult]): String = {
    checkResults
      .flatMap { case (_, checkResult) => checkResult.constraintResults }
      .filter(_.status != ConstraintStatus.Success)
      .map(result => s"${result.constraint} -> ${result.message.get}")
      .mkString("\n")
  }
}

/**
 * Companion object for creating instances of `BigQueryMetricsRepository`.
 *
 * @author david.christianto
 */
object BigQueryMetricsRepository {

  /**
   * Factory method to create a new instance of `BigQueryMetricsRepository`.
   *
   * @param spark                 The `SparkSession` instance.
   * @param deequRepositoryConfig Deequ configuration details for BigQuery integration.
   * @return A new instance of `BigQueryMetricsRepository`.
   */
  def apply(spark: SparkSession, deequRepositoryConfig: BigQueryConfig): BigQueryMetricsRepository = {
    new BigQueryMetricsRepository(spark, deequRepositoryConfig)
  }
}