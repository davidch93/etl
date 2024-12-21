package com.github.davidch93.etl.batch.writers

import com.github.davidch93.etl.core.config.BigQueryConfig
import com.github.davidch93.etl.core.constants.Dataset
import com.github.davidch93.etl.core.constants.MetadataField.AUDIT_WRITE_TIME
import com.github.davidch93.etl.core.schema.Table
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.JavaConverters._

/**
 * A utility class for writing a `DataFrame` to BigQuery with support for configurations like
 * partitioning, clustering, and managing GCS temporary storage paths.
 *
 * Scala example to write `DataFrame` to BigQuery:
 * {{{
 *   BigQueryWriter
 *     .write(dataFrame)
 *     .withGcsBucket(gcsBucket)
 *     .withGcsPath(gcsPath)
 *     .withBigQueryConfig(bigQueryConfig)
 *     .withTable(tableSchema)
 *     .writeToBigQuery()
 * }}}
 *
 * @param dataFrame      The input `DataFrame` to be written to BigQuery.
 * @param gcsBucket      The GCS bucket used for persistent storage during the BigQuery write.
 * @param gcsPath        The GCS path used for persistent storage during the BigQuery write.
 * @param bigQueryConfig The configuration settings for the BigQuery write operation.
 * @param table          The table schema and configuration for the target BigQuery table.
 */
class BigQueryWriter private(
  dataFrame: DataFrame,
  gcsBucket: String,
  gcsPath: String,
  bigQueryConfig: BigQueryConfig,
  table: Table
) {

  /**
   * Executes a write operation to BigQuery in truncate mode, replacing the existing data.
   * Handles partitioning and clustering if specified in the table schema.
   */
  private def writeToBigQuery(): Unit = {
    deleteGcsPathIfExists()

    var bigQueryOptions = Map(
      "createDisposition" -> bigQueryConfig.getCreateDisposition,
      "persistentGcsBucket" -> gcsBucket.replace("gs://", ""),
      "persistentGcsPath" -> gcsPath
    )

    val tablePartitionOpt = Option(table.getTablePartition.orElse(null))

    if (tablePartitionOpt.isDefined && !table.getClusteredColumns.isEmpty) {
      bigQueryOptions ++= Map(
        "partitionField" -> tablePartitionOpt.get.getPartitionColumn,
        "partitionType" -> tablePartitionOpt.get.getPartitionType,
        "clusteredFields" -> table.getClusteredColumns.asScala.mkString(",")
      )
    } else if (tablePartitionOpt.isDefined) {
      bigQueryOptions ++= Map(
        "partitionField" -> tablePartitionOpt.get.getPartitionColumn,
        "partitionType" -> tablePartitionOpt.get.getPartitionType
      )
    } else if (!table.getClusteredColumns.isEmpty) {
      bigQueryOptions += ("clusteredFields" -> table.getClusteredColumns.asScala.mkString(","))
    }

    val bqTableName = bigQueryConfig.getFullyQualifiedTableName(Dataset.DAILY, table.getSource, table.getName)
    dataFrame
      .withColumn(AUDIT_WRITE_TIME, current_timestamp())
      .write
      .mode(SaveMode.Overwrite)
      .format("bigquery")
      .options(bigQueryOptions)
      .save(bqTableName)
  }

  /**
   * Deletes the temporary GCS path if it already exists, ensuring a clean state for the write operation.
   *
   * The Spark BigQuery connector cannot override data in the GCS path. It will throw an Exception if it exists.
   * Therefore, the job must delete the path before rewriting data to BigQuery.
   */
  private def deleteGcsPathIfExists(): Unit = {
    val dataPoolPath = f"$gcsBucket/$gcsPath"
    val path = new Path(dataPoolPath)
    val fileSystem = path.getFileSystem(new Configuration())
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }
}

/**
 * Companion object for constructing and executing BigQueryWriter operations.
 *
 * @author david.christianto
 */
object BigQueryWriter {

  /**
   * Creates a `BigQueryWriterBuilder` for writing a `DataFrame` to BigQuery.
   *
   * @param dataFrame The input `DataFrame` to write.
   * @return A `BigQueryWriterBuilder` instance for further configuration.
   */
  def write(dataFrame: DataFrame): BigQueryWriterBuilder = {
    BigQueryWriterBuilder(dataFrame)
  }

  /**
   * A builder class for configuring and executing `BigQueryWriter` operations.
   *
   * @param dataFrame      The input `DataFrame`.
   * @param gcsBucket      The GCS bucket for persistent storage.
   * @param gcsPath        The GCS persistent storage path.
   * @param bigQueryConfig The configuration settings for BigQuery.
   * @param table          The schema and settings for the target BigQuery table.
   */
  case class BigQueryWriterBuilder private(
    dataFrame: DataFrame,
    gcsBucket: String = "",
    gcsPath: String = "",
    bigQueryConfig: BigQueryConfig = new BigQueryConfig(),
    table: Table = new Table()
  ) {

    /**
     * Specifies the GCS bucket for persistent storage.
     *
     * @param gcsBucket The GCS bucket name.
     * @return An updated `BigQueryWriterBuilder` instance.
     */
    def withGcsBucket(gcsBucket: String): BigQueryWriterBuilder =
      copy(gcsBucket = gcsBucket)

    /**
     * Specifies the GCS path for persistent storage.
     *
     * @param gcsPath The GCS path.
     * @return An updated `BigQueryWriterBuilder` instance.
     */
    def withGcsPath(gcsPath: String): BigQueryWriterBuilder =
      copy(gcsPath = gcsPath)

    /**
     * Specifies the BigQuery configuration for the write operation.
     *
     * @param bigQueryConfig The `BigQueryConfig` instance.
     * @return An updated `BigQueryWriterBuilder` instance.
     */
    def withBigQueryConfig(bigQueryConfig: BigQueryConfig): BigQueryWriterBuilder =
      copy(bigQueryConfig = bigQueryConfig)

    /**
     * Specifies the schema and settings for the target BigQuery table.
     *
     * @param table The `Table` instance.
     * @return An updated `BigQueryWriterBuilder` instance.
     */
    def withTable(table: Table): BigQueryWriterBuilder =
      copy(table = table)

    /**
     * Executes the write operation to BigQuery in truncate mode.
     */
    def writeToBigQuery(): Unit =
      new BigQueryWriter(dataFrame, gcsBucket, gcsPath, bigQueryConfig, table).writeToBigQuery()
  }
}