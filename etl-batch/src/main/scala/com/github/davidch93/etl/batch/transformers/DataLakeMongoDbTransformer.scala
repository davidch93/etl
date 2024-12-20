package com.github.davidch93.etl.batch.transformers

import com.fasterxml.jackson.databind.node.ObjectNode
import com.github.davidch93.etl.core.constants.MetadataField._
import com.github.davidch93.etl.core.utils.JsonUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Object responsible for transforming MongoDB data ingested from Kafka into two formats:
 *  - Raw Data Lake format
 *  - Curated Data Lake format
 *
 * This transformation extracts necessary fields, applies schema structures, and formats data
 * to ensure compatibility for downstream processing.
 *
 * @author david.christianto
 */
object DataLakeMongoDbTransformer {

  private val extractedSourceColumns = Array(
    get_json_object(col(PAYLOAD_SOURCE), s"$$.$PAYLOAD_SOURCE_TS_MS").cast(LongType).as(TS_MS),
    get_json_object(col(PAYLOAD_SOURCE), s"$$.$PAYLOAD_SOURCE_ORD").cast(LongType).as(ORD)
  )

  /**
   * Extracts MongoDB payload in the following order
   *   1. Handle `$set` (MongoDB 3 & 4)
   *   1. Handle `diff` with `u` and `i` (Mongo 5 or above)
   *   1. Handle `diff` with `u` only (Mongo 5 or above)
   *   1. Handle `diff` with `i` only (Mongo 5 or above)
   *   1. Create an empty JSON with `_id` information only.
   */
  private val extractPatchPayloadUDF: UserDefinedFunction = udf((jsonString: String, id: String) => {
    val payload = JsonUtils.toJsonNode(jsonString)
    val oid = JsonUtils.toJsonNode(id)
    if (payload.has(PAYLOAD_SET)) {
      payload.get(PAYLOAD_SET).asInstanceOf[ObjectNode].set("_id", oid).toString
    } else if (payload.has(PAYLOAD_DIFF) && payload.get(PAYLOAD_DIFF).has(PAYLOAD_DIFF_U) && payload.get(PAYLOAD_DIFF).has(PAYLOAD_DIFF_I)) {
      val newPayload = JsonUtils.createEmptyObjectNode()
      newPayload.setAll(payload.get(PAYLOAD_DIFF).get(PAYLOAD_DIFF_U).asInstanceOf[ObjectNode])
      newPayload.setAll(payload.get(PAYLOAD_DIFF).get(PAYLOAD_DIFF_I).asInstanceOf[ObjectNode])
      newPayload.set("_id", oid).toString
    } else if (payload.has(PAYLOAD_DIFF) && payload.get(PAYLOAD_DIFF).has(PAYLOAD_DIFF_U)) {
      payload.get(PAYLOAD_DIFF).get(PAYLOAD_DIFF_U).asInstanceOf[ObjectNode].set("_id", oid).toString
    } else if (payload.has(PAYLOAD_DIFF) && payload.get(PAYLOAD_DIFF).has(PAYLOAD_DIFF_I)) {
      payload.get(PAYLOAD_DIFF).get(PAYLOAD_DIFF_I).asInstanceOf[ObjectNode].set("_id", oid).toString
    } else {
      payload.asInstanceOf[ObjectNode].set("_id", oid).toString
    }
  })

  /**
   * Extracts the payload for delete (`d`) events, creating an empty JSON object with `_id` only.
   */
  private val extractDeletedPayloadUDF: UserDefinedFunction = udf((jsonString: String) => {
    val oid = JsonUtils.toJsonNode(jsonString)
    JsonUtils.createEmptyObjectNode().set("_id", oid).toString
  })

  /**
   * Transforms a Kafka DataFrame containing MongoDB change events into a Raw Data Lake format.
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
   * @param kafkaDataFrame The input DataFrame containing MongoDB CDC events in JSON format from Kafka.
   * @return A DataFrame with fields extracted and formatted for the Raw Data Lake.
   */
  def transformToRawDataLake(kafkaDataFrame: DataFrame): DataFrame = {
    import kafkaDataFrame.sparkSession.implicits._

    kafkaDataFrame
      .withColumn("_id", get_json_object($"$KEY", f"$$.$PAYLOAD.$PAYLOAD_ID"))
      .withColumn(PAYLOAD_TS_MS, get_json_object($"$VALUE", f"$$.$PAYLOAD.$PAYLOAD_TS_MS").cast(LongType))
      .withColumn(PAYLOAD_OP, get_json_object($"$VALUE", f"$$.$PAYLOAD.$PAYLOAD_OP"))
      .withColumn(PAYLOAD_BEFORE, lit(null).cast(StringType))
      .withColumn(PAYLOAD_AFTER, when($"$PAYLOAD_OP" === PAYLOAD_OP_R || $"$PAYLOAD_OP" === PAYLOAD_OP_C, get_json_object($"$VALUE", f"$$.$PAYLOAD.$PAYLOAD_AFTER"))
        .when($"$PAYLOAD_OP" === PAYLOAD_OP_U, extractPatchPayloadUDF(get_json_object($"$VALUE", f"$$.$PAYLOAD.$PAYLOAD_PATCH"), $"_id"))
        .when($"$PAYLOAD_OP" === PAYLOAD_OP_D, extractDeletedPayloadUDF($"_id")))
      .withColumn(PAYLOAD_SOURCE, get_json_object($"$VALUE", f"$$.$PAYLOAD.$PAYLOAD_SOURCE"))
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
   *    |   |-- _ord: long (nullable = false)
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
    rawLakeDataFrame
      .withColumn(STRUCT_SOURCE, struct(extractedSourceColumns: _*))
      .withColumn(STRUCT_DATA, struct(buildStructColumns(tableSchema): _*))
  }

  /**
   * Constructs an array of Columns for the structured 'data' field based on the table schema.
   *
   * For example: Input
   * {{{
   *   {
   *     "id": "{\"$oid\":\"630b23db6ef80123d6e72d74\"}",
   *     "name": "data",
   *     "member_id": {
   *       "$numberLong": 123456789
   *     },
   *     "created_at": {
   *       "$date": 1662085428734
   *     },
   *     "updated_at": {
   *       "$date": "1662085428734"
   *     }
   *   }
   * }}}
   *
   * Output
   * {{{
   *   {
   *     "id": "{\"$oid\":\"630b23db6ef80123d6e72d74\"}",
   *     "name": "data",
   *     "member_id": 123456789
   *     "created_at": 1662085428734
   *     "updated_at": 1662085428734
   *   }
   * }}}
   *
   * @param tableSchema The target table schema, defining the fields to be extracted from JSON data.
   * @return An array of Columns with values extracted and cast according to the schema.
   */
  private def buildStructColumns(tableSchema: StructType): Array[Column] = {
    tableSchema.fields.map { field =>
      val column = get_json_object(col(PAYLOAD_AFTER), f"$$.${field.name}")
      when(column.isNull, null).otherwise(
        coalesce(
          get_json_object(column, "$.$date"),
          get_json_object(column, "$.$numberInt"),
          get_json_object(column, "$.$numberLong"),
          get_json_object(column, "$.$numberDecimal"),
          column
        ).cast(field.dataType)
      ).as(field.name)
    }
  }
}
