package com.github.davidch93.etl.batch.transformers

import com.github.davidch93.etl.core.constants.MetadataField._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Object responsible for transforming MySQL data ingested from Kafka into two formats:
 *  - Raw Data Lake format
 *  - Curated Data Lake format
 *
 * This transformation extracts necessary fields, applies schema structures, and formats data
 * to ensure compatibility for downstream processing.
 *
 * @author david.christianto
 */
object DataLakeMySqlTransformer {

  private val extractedSourceColumns = Array(
    get_json_object(col(PAYLOAD_SOURCE), s"$$.$PAYLOAD_SOURCE_TS_MS").cast(LongType).as(TS_MS),
    get_json_object(col(PAYLOAD_SOURCE), s"$$.$PAYLOAD_SOURCE_FILE").as(FILE),
    get_json_object(col(PAYLOAD_SOURCE), s"$$.$PAYLOAD_SOURCE_POS").cast(LongType).as(POS)
  )

  /**
   * Transforms a Kafka DataFrame containing MySQL change events into a Raw Data Lake format.
   *
   * The resulting DataFrame has the following schema:
   * {{{
   *   root
   *    |-- ts_ms: long (nullable = false)
   *    |-- op: string (nullable = false)
   *    |-- before: string (nullable = true)
   *    |-- after: string (nullable = true)
   *    |-- source: string (nullable = false)
   *    |-- _datetime_partition: string (nullable = false)
   * }}}
   *
   * @param kafkaDataFrame The input DataFrame containing MySQL CDC events in JSON format from Kafka.
   * @return A DataFrame with fields extracted and formatted for the Raw Data Lake.
   */
  def transformToRawDataLake(kafkaDataFrame: DataFrame): DataFrame = {
    import kafkaDataFrame.sparkSession.implicits._

    kafkaDataFrame
      .withColumn(PAYLOAD_TS_MS, get_json_object($"$VALUE", s"$$.$PAYLOAD.$PAYLOAD_TS_MS").cast(LongType))
      .withColumn(PAYLOAD_OP, get_json_object($"$VALUE", s"$$.$PAYLOAD.$PAYLOAD_OP"))
      .withColumn(PAYLOAD_BEFORE, get_json_object($"$VALUE", s"$$.$PAYLOAD.$PAYLOAD_BEFORE"))
      .withColumn(PAYLOAD_AFTER, get_json_object($"$VALUE", s"$$.$PAYLOAD.$PAYLOAD_AFTER"))
      .withColumn(PAYLOAD_SOURCE, get_json_object($"$VALUE", s"$$.$PAYLOAD.$PAYLOAD_SOURCE"))
      .withColumn(TS_MS, get_json_object($"$PAYLOAD_SOURCE", s"$$.$PAYLOAD_SOURCE_TS_MS").cast(LongType))
      .withColumn(DATE_TIME_PARTITION, from_unixtime($"$TS_MS" / 1000L, DATE_HOUR_FORMAT))
      .select(
        $"$PAYLOAD_TS_MS",
        $"$PAYLOAD_OP",
        $"$PAYLOAD_BEFORE",
        $"$PAYLOAD_AFTER",
        $"$PAYLOAD_SOURCE",
        $"$DATE_TIME_PARTITION"
      )
  }

  /**
   * Transforms a Raw Data Lake DataFrame into a Curated Data Lake format by structuring and flattening data.
   *
   * The resulting DataFrame has the following schema:
   * {{{
   *   root
   *    |-- _struct_source: struct (nullable = false)
   *    |   |-- _ts_ms: long (nullable = false)
   *    |   |-- _file: string (nullable = false)
   *    |   |-- _pos: long (nullable = false)
   *    |-- _struct_data: struct (nullable = false)
   *    |   |-- id: long (nullable = true)
   *    |   |-- ...                                 # Other fields inferred from the table schema
   * }}}
   *
   * @param rawLakeDataFrame The input DataFrame formatted for the Raw Data Lake.
   * @param tableSchema      The schema of the target table, used to infer fields in the 'data' struct.
   * @return A DataFrame formatted for the Curated Data Lake with structured source and data columns.
   */
  def transformToCuratedDataLake(rawLakeDataFrame: DataFrame, tableSchema: StructType): DataFrame = {
    import rawLakeDataFrame.sparkSession.implicits._

    rawLakeDataFrame
      .withColumn(STRUCT_SOURCE, struct(extractedSourceColumns: _*))
      .withColumn(JSON_DATA, when($"$PAYLOAD_OP" === PAYLOAD_OP_D, $"$PAYLOAD_BEFORE").otherwise($"$PAYLOAD_AFTER"))
      .withColumn(STRUCT_DATA, struct(buildStructColumns(tableSchema): _*))
  }

  /**
   * Constructs an array of Columns for the structured 'data' field based on the table schema.
   *
   * @param tableSchema The target table schema, defining the fields to be extracted from JSON data.
   * @return An array of Columns with values extracted and cast according to the schema.
   */
  private def buildStructColumns(tableSchema: StructType): Array[Column] = {
    tableSchema.fields.map { field =>
      get_json_object(col(JSON_DATA), f"$$.${field.name}").cast(field.dataType).as(field.name)
    }
  }
}
