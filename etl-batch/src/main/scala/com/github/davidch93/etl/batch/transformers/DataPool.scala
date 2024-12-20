package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.batch.helpers.SchemaConverter
import com.github.davidch93.etl.core.constants.Source
import com.github.davidch93.etl.core.schema.Table
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
 * A transformer class for transforming and deduplicating data in a data pool.
 * It supports metadata column addition, record deduplication, and schema-based loading of data
 * for various data sources (e.g., MySQL, PostgreSQL, MongoDB, DynamoDB).
 *
 * Scala example to get data pool only:
 * {{{
 *   DataPool.forPath(dataPoolPath)
 *     .mergeWith(lakeDataFrame)
 *     .withTable(tableSchema)
 *     .load()
 * }}}
 *
 * @param spark         the active `SparkSession`.
 * @param dataPoolPath  the file path to the data pool (parquet files).
 * @param lakeDataFrame an optional `DataFrame` representing lake data to merge with the data pool.
 * @param table         the table schema and metadata defining the structure of the data pool.
 * @author david.christianto
 */
class DataPool private(
  spark: SparkSession,
  dataPoolPath: String,
  lakeDataFrame: DataFrame,
  table: Table,
) {

  private val addMetadataColumns: DataFrame => DataFrame = inputDataFrame =>
    table.getSource match {
      case Source.MYSQL => DataPoolMySqlTransformer.addMetadataColumns(inputDataFrame)
      case Source.POSTGRESQL => DataPoolPostgreSqlTransformer.addMetadataColumns(inputDataFrame)
      case Source.MONGODB => DataPoolMongoDbTransformer.addMetadataColumns(inputDataFrame)
      case Source.DYNAMODB => DataPoolDynamoDbTransformer.addMetadataColumns(inputDataFrame)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported data pool transformation from source: `$other`!")
    }

  private val deduplicateRecords: (DataFrame, List[String]) => DataFrame = (inputDataFrame, primaryKeys) =>
    table.getSource match {
      case Source.MYSQL => DataPoolMySqlTransformer.deduplicateRecords(inputDataFrame, primaryKeys)
      case Source.POSTGRESQL => DataPoolPostgreSqlTransformer.deduplicateRecords(inputDataFrame, primaryKeys)
      case Source.MONGODB => DataPoolMongoDbTransformer.deduplicateRecords(inputDataFrame, primaryKeys)
      case Source.DYNAMODB => DataPoolDynamoDbTransformer.deduplicateRecords(inputDataFrame, primaryKeys)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported data pool transformation from source: `$other`!")
    }

  /**
   * Loads the data from the data pool, merging it with the lake data if provided.
   *
   * Return a DataFrame with the following schema.
   * {{{
   *  root
   *    |-- id: long (nullable = true)
   *    |-- ...                                            # The other fields
   *    |-- order_timestamp: timestamp (nullable = false)  # A partition column if any
   *    |-- _is_deleted: boolean (nullable = false)
   * }}}
   *
   * @return a `DataFrame` representing the loaded and optionally merged data pool.
   */
  private def load(): DataFrame = {
    val tablePartitionOpt = Option(table.getTablePartition.orElse(null))
    val dataPoolSchema = SchemaConverter.tableSchemaWithMetadataToStructType(table.getSchema, tablePartitionOpt)

    val poolDataFrame = spark
      .read
      .schema(dataPoolSchema)
      .parquet(dataPoolPath)

    if (lakeDataFrame == null || lakeDataFrame.isEmpty) {
      poolDataFrame
    } else {
      val unionDataFrame = lakeDataFrame.union(addMetadataColumns(poolDataFrame))
      val primaryKeys = table.getConstraintKeys.asScala.toList

      deduplicateRecords(unionDataFrame, primaryKeys)
    }
  }
}

/**
 * Companion object for creating and configuring a `DataPool` class.
 *
 * @author david.christianto
 */
object DataPool {

  /**
   * Creates a `DataPoolBuilder` for the given data pool path using the active `SparkSession`.
   *
   * @param dataPoolPath the file path to the data pool (parquet files).
   * @return a `DataPoolBuilder` instance for further configuration.
   * @throws IllegalArgumentException if no active `SparkSession` is found.
   */
  def forPath(dataPoolPath: String): DataPoolBuilder = {
    val spark = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession!")
    }
    forPath(spark, dataPoolPath)
  }

  /**
   * Creates a `DataPoolBuilder` for the given data pool path and `SparkSession`.
   *
   * @param spark        the active `SparkSession`.
   * @param dataPoolPath the file path to the data pool (parquet files).
   * @return a `DataPoolBuilder` instance for further configuration.
   */
  def forPath(spark: SparkSession, dataPoolPath: String): DataPoolBuilder = {
    DataPoolBuilder(spark, dataPoolPath)
  }

  /**
   * A builder class for configuring and constructing a `DataPool` instance.
   *
   * @param spark         the active `SparkSession`.
   * @param dataPoolPath  the file path to the data pool (parquet files).
   * @param lakeDataFrame an optional `DataFrame` representing lake data to merge with the data pool.
   * @param table         the table schema and metadata defining the structure of the data pool.
   */
  case class DataPoolBuilder private(
    private val spark: SparkSession,
    private val dataPoolPath: String,
    private val lakeDataFrame: DataFrame = null,
    private val table: Table = new Table()
  ) {

    /**
     * Merges the data pool with the specified lake `DataFrame`.
     *
     * @param lakeDataFrame the `DataFrame` to merge with the data pool.
     * @return a `DataPoolBuilder` with the lake `DataFrame` set.
     */
    def mergeWith(lakeDataFrame: DataFrame): DataPoolBuilder =
      copy(lakeDataFrame = lakeDataFrame)

    /**
     * Specifies the table schema and metadata for the data pool.
     *
     * @param table the `Table` instance defining the schema and metadata.
     * @return a `DataPoolBuilder` with the table set.
     */
    def withTable(table: Table): DataPoolBuilder =
      copy(table = table)

    /**
     * Loads the configured data pool, optionally merging it with lake data and deduplicating records.
     *
     * @return a `DataFrame` representing the loaded and processed data pool.
     */
    def load(): DataFrame =
      new DataPool(spark, dataPoolPath, lakeDataFrame, table).load()
  }
}