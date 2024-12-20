package com.github.davidch93.etl.batch.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * A utility class for writing a DataFrame to Google Cloud Storage (GCS) in Parquet format.
 * The class supports optional partitioning of the data based on specified columns.
 *
 * Scala example to write DataFrame to GCS:
 * {{{
 *   GcsWriter.writeAppend(df)
 *     .withGcsPath(gcsPath)
 *     .withPartitions(Array(partitions))
 *     .execute()
 * }}}
 *
 * @param dataFrame  The input DataFrame to be written to GCS.
 * @param gcsPath    The target GCS path where the data will be stored.
 * @param partitions An array of column names used for partitioning the data.
 *                   If empty or `null`, no partitioning is applied.
 * @author david.christianto
 */
class GcsWriter(dataFrame: DataFrame, gcsPath: String, partitions: Array[String]) {

  /**
   * Executes the write operation to GCS.
   *
   *  - If partitions are specified, the data is written in partitioned Parquet format.
   *  - If no partitions are specified, the data is written as a single Parquet file.
   */
  def execute(): Unit = {
    if (partitions.nonEmpty) {
      dataFrame
        .write
        .mode(SaveMode.Append)
        .partitionBy(partitions: _*)
        .parquet(gcsPath)
    } else {
      dataFrame
        .write
        .mode(SaveMode.Append)
        .parquet(gcsPath)
    }
  }
}

/**
 * Companion object for the `GcsWriter` class, providing a builder pattern to simplify
 * the creation and configuration of a `GcsWriter` instance.
 *
 * @author david.christianto
 */
object GcsWriter {

  /**
   * Initializes a builder for writing a DataFrame to GCS in append mode.
   *
   * @param dataFrame The input DataFrame to be written.
   * @return An instance of `GcsWriterBuilder` for further configuration.
   */
  def writeAppend(dataFrame: DataFrame): GcsWriterBuilder = {
    GcsWriterBuilder(dataFrame)
  }

  /**
   * A builder class for configuring and executing GCS write operations.
   *
   * @param dataFrame  The DataFrame to be written.
   * @param gcsPath    The target GCS path for storing the data.
   * @param partitions An array of column names used for partitioning the data.
   */
  case class GcsWriterBuilder private(
    dataFrame: DataFrame,
    gcsPath: String = "",
    partitions: Array[String] = Array()
  ) {

    /**
     * Sets the GCS path for the write operation.
     *
     * @param gcsPath The target GCS path.
     * @return A new instance of `GcsWriterBuilder` with the updated GCS path.
     */
    def withGcsPath(gcsPath: String): GcsWriterBuilder =
      copy(gcsPath = gcsPath)

    /**
     * Sets the columns for partitioning the data.
     *
     * @param partitions An array of column names used for partitioning.
     * @return A new instance of `GcsWriterBuilder` with the updated partition columns.
     */
    def withPartitions(partitions: Array[String]): GcsWriterBuilder =
      copy(partitions = partitions)

    /**
     * Executes the configured GCS write operation.
     */
    def execute(): Unit = {
      new GcsWriter(dataFrame, gcsPath, partitions).execute()
    }
  }
}