package com.github.davidch93.etl.batch.pipelines

import com.github.davidch93.etl.batch.config.BatchConfigLoader
import com.github.davidch93.etl.batch.constants.Field._
import com.github.davidch93.etl.batch.helpers.PipelineHelper
import com.github.davidch93.etl.batch.transformers.{DataPool, Kafka}
import com.github.davidch93.etl.batch.writers.BigQueryWriter
import com.github.davidch93.etl.core.constants.MetadataField.ROW_NUMBER
import com.github.davidch93.etl.core.pipelines.EtlPipeline
import com.github.davidch93.etl.core.schema.SchemaLoader
import com.github.davidch93.etl.core.utils.{DateTimeUtils, KafkaTopicResolver}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

/**
 * Pipeline to process data from various sources and write it to a data pool and BigQuery.
 *
 * This pipeline validates jobs to be executed, runs them, and maintains an audit history of job execution.
 *
 * @param configPath         Path to the configuration file.
 * @param scheduledTimestamp The timestamp when the pipeline is scheduled to run.
 * @author david.christianto
 */
class DataPoolPipeline(configPath: String, scheduledTimestamp: String) extends EtlPipeline {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val config = BatchConfigLoader.fromConfigPath(configPath)
  private val spark = SparkSession.builder()
    .appName("DataPoolPipeline")
    .getOrCreate()

  private val pipelineHelper = PipelineHelper(config.groupName, config.maxThreads, config.bigQueryConfig)

  private val jobHistoryTable = f"${config.bigQueryConfig.getProjectId}:${config.bigQueryConfig.getDatasetId}_audit.data_pool_job_history"
  private val jobHistorySchema = new StructType()
    .add(TABLE_NAME, StringType, nullable = false)
    .add(GROUP_NAME, StringType, nullable = false)
    .add(START_TIMESTAMP, LongType, nullable = false)
    .add(END_TIMESTAMP, LongType, nullable = false)
    .add(DURATION_SECONDS, LongType, nullable = false)
    .add(IS_SUCCESS, BooleanType, nullable = false)
    .add(MESSAGE, StringType)

  private var previousSnapshotDate: String = _
  private var currentSnapshotDate: String = _
  private var startTimestamp: LocalDateTime = _
  private var endTimestamp: LocalDateTime = _

  /**
   * Prepares the pipeline by setting timestamps and snapshot dates.
   */
  override def prepare(): Unit = {
    startTimestamp = DateTimeUtils.parseIsoLocalDateTime(scheduledTimestamp)
    endTimestamp = startTimestamp.plusDays(1).plusMinutes(15)

    logger.info(f"[ETL-BATCH] Kafka start scheduled timestamp: $startTimestamp")
    logger.info(f"[ETL-BATCH] Kafka end scheduled timestamp: $endTimestamp")

    previousSnapshotDate = startTimestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    currentSnapshotDate = startTimestamp.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    logger.info(f"[ETL-BATCH] Data Pool previous snapshot date: $previousSnapshotDate")
    logger.info(f"[ETL-BATCH] Data Pool current snapshot date: $currentSnapshotDate")
  }

  /**
   * Executes the pipeline, ensuring all jobs run successfully.
   *
   * @throws Exception if there are any failed jobs
   */
  override def start(): Unit = {
    logger.info(s"[ETL-BATCH] Starting jobs for group: `${config.groupName}`.")

    val filterExpr = s"DATE($SCHEDULED_TIMESTAMP) = '$currentSnapshotDate' AND $GROUP_NAME = '${config.groupName}'"
    val allJobsSuccessful = pipelineHelper.executeJobs(
      validatePendingJobs(filterExpr),
      executeJob,
      recordJobHistory
    )

    if (!allJobsSuccessful) {
      notifyOnFailedJobs(s"DATE($SCHEDULED_TIMESTAMP) = '$currentSnapshotDate' " +
        s"AND $GROUP_NAME = '${config.groupName}' " +
        s"AND $IS_SUCCESS = false"
      )
      throw new Exception(s"Some jobs failed for group `${config.groupName}` on $scheduledTimestamp!")
    }

    logger.info(s"[ETL-BATCH] All jobs for group `${config.groupName}` completed successfully.")
  }

  /**
   * Validates and filters the jobs that need to be executed.
   *
   * This method checks the job histories in BigQuery and compares them against the whitelist of tables.
   * It identifies which jobs were successful and which ones still need to be run.
   *
   * @param filterCondition The filter expression to identify jobs in BigQuery.
   * @return Array of job identifiers (Kafka topics) to execute.
   */
  private def validatePendingJobs(filterCondition: String): Array[String] = {
    logger.info("[ETL-BATCH] Validating the list of jobs that need to be run.")

    val jobHistoriesDataFrame = spark
      .read
      .format("bigquery")
      .option("filter", filterCondition)
      .load(jobHistoryTable)

    val latestJobWindow = Window
      .partitionBy(col(TABLE_NAME))
      .orderBy(col(END_TIMESTAMP).desc)

    val completedJobs = jobHistoriesDataFrame
      .withColumn(ROW_NUMBER, row_number().over(latestJobWindow))
      .filter(col(ROW_NUMBER) === 1 && col(IS_SUCCESS) === true)
      .select(col(TABLE_NAME))
      .collect()
      .map(_.getAs[String](TABLE_NAME))

    config.tableWhitelist.toBuffer.diff(completedJobs).toArray
  }

  /**
   * Executes the main ETL job for a given Kafka topic.
   *
   * @param kafkaTopic The Kafka topic to process.
   */
  private def executeJob(kafkaTopic: String): Unit = {
    spark.sparkContext.setJobGroup(kafkaTopic, s"Processing ETL jobs for topic: $kafkaTopic")

    val resolver = KafkaTopicResolver.resolve(kafkaTopic)
    val source = resolver.identifySource().toString.toLowerCase
    val schemaFilePath = s"${config.configBucket}/schema/$source/${resolver.getDatabaseName}_${resolver.getTableName}/schema.json"
    val tableSchema = SchemaLoader.loadTableSchema(schemaFilePath)

    val dataLakePath = s"${config.dataLakeBucket}/$source/${tableSchema.getName}"
    val (rawLakeDataFrame, curatedLakeDataFrame) = Kafka.read(kafkaTopic)
      .withKafkaBootstrapServers(config.kafkaConfig.getBootstrapServers)
      .withTimeRange(startTimestamp, endTimestamp)
      .withTable(tableSchema)
      .withDataLakePath(dataLakePath)
      .loadAndWriteDataLake()

    val dataPoolPath = s"${config.dataPoolBucket}/$source/${tableSchema.getName}/snapshot=$previousSnapshotDate"
    val poolDataFrame = DataPool.forPath(spark, dataPoolPath)
      .mergeWith(curatedLakeDataFrame)
      .withTable(tableSchema)
      .load()

    val gcsPath = s"$source/${tableSchema.getName}/snapshot=$currentSnapshotDate"
    BigQueryWriter.write(poolDataFrame)
      .withGcsBucket(config.dataPoolBucket)
      .withGcsPath(gcsPath)
      .withBigQueryConfig(config.bigQueryConfig)
      .withTable(tableSchema)
      .writeToBigQuery()

    rawLakeDataFrame.unpersist()
  }

  /**
   * Records the job execution history in BigQuery.
   *
   * @param jobResult Result row of the executed job.
   * {{{
   *    Row(
   *      tableName: String,           # The table name
   *      startEpochSeconds: long,     # The job start timestamp in epoch seconds
   *      endEpochSeconds: long,       # The job end timestamp in epoch seconds
   *      durationSeconds: int,        # Duration of the job
   *      isSuccess: boolean,          # The success status of the job
   *      message: string              # The message given from the job
   *    )
   * }}}
   */
  private def recordJobHistory(jobResult: Row): Unit = {
    logger.info(s"[ETL-BATCH] Recording job history for timestamp: $scheduledTimestamp.")

    val jobHistoriesDF = spark
      .createDataFrame(spark.sparkContext.parallelize(List(jobResult)), jobHistorySchema)
      .withColumn(GROUP_NAME, lit(config.groupName))
      .withColumn(SCHEDULED_TIMESTAMP, lit(startTimestamp.toEpochSecond(ZoneOffset.UTC)).cast(TimestampType))
      .withColumn(START_TIMESTAMP, col(START_TIMESTAMP).cast(TimestampType))
      .withColumn(END_TIMESTAMP, col(END_TIMESTAMP).cast(TimestampType))

    val tempGcs = f"${config.bigQueryConfig.getTemporaryGcsBucket}/data_pool_job_history/${jobResult.getAs[String](TABLE_NAME)}/date=$currentSnapshotDate"
    jobHistoriesDF
      .write
      .mode(SaveMode.Append)
      .format("bigquery")
      .option("createDisposition", config.bigQueryConfig.getCreateDisposition)
      .option("temporaryGcsBucket", tempGcs.replace("gs://", ""))
      .option("partitionField", SCHEDULED_TIMESTAMP)
      .save(jobHistoryTable)
  }

  /**
   * Notifies about failed jobs via email.
   *
   * This method retrieves the failed jobs from the job history table in BigQuery and sends notification
   * emails containing the details of these failed jobs.
   *
   * @param filterCondition The filter expression to be applied when querying failed jobs.
   */
  private def notifyOnFailedJobs(filterCondition: String): Unit = {
    logger.info("[ETL-BATCH] Notifying the failed jobs to the email!")

    val failedJobs = spark
      .read
      .format("bigquery")
      .option("filter", filterCondition)
      .load(jobHistoryTable)
      .select(col(TABLE_NAME), col(MESSAGE))
      .collect()

    pipelineHelper.notifyOnFailedJobs(failedJobs, scheduledTimestamp)
  }
}
