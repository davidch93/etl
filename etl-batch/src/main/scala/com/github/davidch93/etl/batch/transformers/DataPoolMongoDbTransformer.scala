package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.core.constants.MetadataField._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{last, lit, row_number}

/**
 * Object responsible for transforming MongoDB data into a consistent structure for data pooling
 * and for deduplicating data based on specified constraints.
 *
 * @author david.christianto
 */
object DataPoolMongoDbTransformer {

  /**
   * Adds metadata columns (`TS_MS` and `ORD`) to the given `DataFrame`.
   *
   * Return a `DataFrame` with the following schema.
   * {{{
   *  root
   *    |-- id: long (nullable = true)
   *    |-- ...                              # The other fields based on the table schema
   *    |-- _ts_ms: long (nullable = false)
   *    |-- _ord: long (nullable = false)
   * }}}
   *
   * @param inputDataFrame the input `DataFrame` to be transformed.
   * @return a `DataFrame` with additional metadata columns.
   */
  def addMetadataColumns(inputDataFrame: DataFrame): DataFrame = {
    inputDataFrame
      .withColumn(TS_MS, lit(0))
      .withColumn(ORD, lit(0))
  }

  /**
   * Deduplicates records in a DataFrame based on the specified primary keys.
   * For duplicate records, retains the latest record for each unique combination of keys.
   * Aggregates non-key columns to retain their most recent non-null values.
   *
   * @param inputDataFrame the DataFrame containing records to deduplicate.
   * @param primaryKeys    the list of primary key column names used for deduplication.
   * @return a deduplicated DataFrame with metadata columns removed.
   */
  def deduplicateRecords(inputDataFrame: DataFrame, primaryKeys: List[String]): DataFrame = {
    import inputDataFrame.sparkSession.implicits._

    val primaryKeysColumns = primaryKeys.map(keys => $"$keys")

    val aggregationWindow = Window
      .partitionBy(primaryKeysColumns: _*)
      .orderBy($"$TS_MS".asc, $"$ORD".asc)

    val deduplicationWindow = Window
      .partitionBy(primaryKeysColumns: _*)
      .orderBy($"$TS_MS".desc, $"$ORD".desc)

    val excludedColumns = primaryKeys ++ TS_MS ++ ORD
    val aggregatedColumns = inputDataFrame.schema.fieldNames
      .filterNot(excludedColumns.contains(_))
      .map(colName => last(colName, ignoreNulls = true).over(aggregationWindow).as(colName))

    val fullColumns = Seq(row_number().over(deduplicationWindow).as(ROW_NUMBER)) ++ primaryKeysColumns ++ aggregatedColumns

    inputDataFrame
      .select(fullColumns: _*)
      .filter($"$ROW_NUMBER" === 1)
      .drop(ROW_NUMBER, TS_MS, ORD)
  }
}
