package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.core.constants.MetadataField._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}

/**
 * Object responsible for transforming DynamoDB data into a consistent structure for data pooling
 * and for deduplicating data based on specified constraints.
 *
 * @author david.christianto
 */
object DataPoolDynamoDbTransformer {

  /**
   * Adds metadata columns (`TS_MS`, `SHARD_ID`, and `SEQUENCE_NUMBER`) to the given `DataFrame`.
   *
   * Return a `DataFrame` with the following schema.
   * {{{
   *  root
   *    |-- id: long (nullable = true)
   *    |-- ...                              # The other fields based on the table schema
   *    |-- _ts_ms: long (nullable = false)
   *    |-- _shard_id: string (nullable = false)
   *    |-- _sequence_number: string (nullable = false)
   * }}}
   *
   * @param inputDataFrame the input `DataFrame` to be transformed.
   * @return a `DataFrame` with additional metadata columns.
   */
  def addMetadataColumns(inputDataFrame: DataFrame): DataFrame = {
    inputDataFrame
      .withColumn(TS_MS, lit(0))
      .withColumn(SHARD_ID, lit(""))
      .withColumn(SEQUENCE_NUMBER, lit(""))
  }

  /**
   * Deduplicates records in a `DataFrame` based on specified constraint keys.
   * Retains the latest record for each unique combination of constraint keys,
   * determined by metadata columns `TS_MS`, `SHARD_ID`, and `SEQUENCE_NUMBER`.
   *
   * @param inputDataFrame the `DataFrame` containing records to deduplicate.
   * @param primaryKeys    the list of primary key column names used for deduplication.
   * @return a deduplicated `DataFrame` with metadata columns removed.
   */
  def deduplicateRecords(inputDataFrame: DataFrame, primaryKeys: List[String]): DataFrame = {
    import inputDataFrame.sparkSession.implicits._

    val primaryKeysColumns = primaryKeys.map(keys => $"$keys")
    val window = Window
      .partitionBy(primaryKeysColumns: _*)
      .orderBy($"$TS_MS".desc, $"$SHARD_ID".desc, $"$SEQUENCE_NUMBER".desc)

    inputDataFrame
      .withColumn(ROW_NUMBER, row_number().over(window))
      .filter($"$ROW_NUMBER" === 1)
      .drop(ROW_NUMBER, TS_MS, SHARD_ID, SEQUENCE_NUMBER)
  }
}
