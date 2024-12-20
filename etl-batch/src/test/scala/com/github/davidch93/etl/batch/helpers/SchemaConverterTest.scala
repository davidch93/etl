package com.github.davidch93.etl.batch.helpers

import com.github.davidch93.etl.core.constants.MetadataField.IS_DELETED
import com.github.davidch93.etl.core.schema.SchemaLoader
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaConverterTest extends AnyFunSuite with Matchers {

  test("Converting to StructType should convert a valid TableSchema to StructType") {
    val schemaFilePath = "src/test/resources/schema/mysql/github_staging_orders/schema.json"
    val table = SchemaLoader.loadTableSchema(schemaFilePath)

    val expectedSchema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("amount", DoubleType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("is_expired", BooleanType, nullable = true),
        StructField("created_at", LongType, nullable = false),
      )
    )

    val result = SchemaConverter.tableSchemaToStructType(table.getSchema)
    result shouldEqual expectedSchema
  }

  test("Converting to StructType should IllegalArgumentException when a table schema is null") {
    intercept[IllegalArgumentException] {
      SchemaConverter.tableSchemaToStructType(null)
    }.getMessage should include("The table schema cannot be null!")
  }

  test("Converting to StructType should add IS_DELETED column if tablePartition is None") {
    val schemaFilePath = "src/test/resources/schema/mysql/github_staging_orders/schema.json"
    val table = SchemaLoader.loadTableSchema(schemaFilePath)

    val expectedSchema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("amount", DoubleType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("is_expired", BooleanType, nullable = true),
        StructField("created_at", LongType, nullable = false),
        StructField(IS_DELETED, BooleanType, nullable = false),
      )
    )

    val result = SchemaConverter.tableSchemaWithMetadataToStructType(table.getSchema)
    result shouldEqual expectedSchema
  }

  test("Converting to StructType should add partition and IS_DELETED columns if tablePartition is present") {
    val schemaFilePath = "src/test/resources/schema/mysql/github_staging_orders/schema.json"
    val table = SchemaLoader.loadTableSchema(schemaFilePath)

    val expectedSchema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("amount", DoubleType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("is_expired", BooleanType, nullable = true),
        StructField("created_at", LongType, nullable = false),
        StructField("order_timestamp", TimestampType, nullable = false),
        StructField(IS_DELETED, BooleanType, nullable = false),
      )
    )

    val tablePartitionOpt = Option(table.getTablePartition.orElse(null))
    val result = SchemaConverter.tableSchemaWithMetadataToStructType(table.getSchema, tablePartitionOpt)

    result shouldEqual expectedSchema
  }
}
