package com.github.davidch93.etl.batch.helpers

import com.github.davidch93.etl.core.constants.MetadataField.IS_DELETED
import com.github.davidch93.etl.core.schema.Field.FieldType
import com.github.davidch93.etl.core.schema.{TablePartition, TableSchema}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
 * Utility object for converting `TableSchema` and `TablePartition` metadata to Spark SQL `StructType` schemas.
 *
 * @author david.christianto
 */
object SchemaConverter {

  /**
   * Converts a `TableSchema` instance to a Spark SQL `StructType`.
   *
   * @param tableSchema The table schema containing field definitions.
   * @return A `StructType` representation of the table schema.
   * @throws IllegalArgumentException if the table schema contains unsupported field types.
   */
  def tableSchemaToStructType(tableSchema: TableSchema): StructType = {
    require(tableSchema != null, "The table schema cannot be null!")

    val structFields = tableSchema.getFields.asScala.map { field =>
      StructField(field.getName, convertFieldTypeToDataType(field.getType), field.isNullable)
    }.toArray

    new StructType(structFields)
  }

  /**
   * Converts a `TableSchema` to a Spark SQL `StructType` with optional metadata columns.
   *
   * @param tableSchema    The table schema containing field definitions. Must not be null.
   * @param tablePartition An optional table partition containing metadata for partition columns.
   * @return A `StructType` representation of the table schema with metadata columns.
   */
  def tableSchemaWithMetadataToStructType(
    tableSchema: TableSchema,
    tablePartition: Option[TablePartition] = None
  ): StructType = {
    val baseSchema = tableSchemaToStructType(tableSchema)

    tablePartition match {
      case Some(partition) =>
        baseSchema
          .add(partition.getPartitionColumn, TimestampType, nullable = false)
          .add(IS_DELETED, BooleanType, nullable = false)
      case None =>
        baseSchema
          .add(IS_DELETED, BooleanType, nullable = false)
    }
  }

  /**
   * Converts a `FieldType` to a corresponding Spark SQL `DataType`.
   *
   * @param fieldType The field type to be converted.
   * @return The corresponding Spark SQL `DataType`.
   * @throws IllegalArgumentException if the field type is unsupported.
   */
  private def convertFieldTypeToDataType(fieldType: FieldType): DataType = {
    fieldType match {
      case FieldType.BOOLEAN => BooleanType
      case FieldType.INTEGER => LongType
      case FieldType.DOUBLE => DoubleType
      case FieldType.STRING => StringType
      case _ => throw new IllegalArgumentException(s"Cannot handle type: `$fieldType`!")
    }
  }
}
