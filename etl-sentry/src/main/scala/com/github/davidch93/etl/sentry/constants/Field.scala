package com.github.davidch93.etl.sentry.constants

/**
 * An object containing constants for tracking the data quality metrics of ETL jobs.
 *
 * <strong>Note:</strong>
 * This object is immutable and all fields are declared as `final` for ease of use.
 *
 * @author david.christianto
 */
object Field {

  final val VALIDATION_DATE = "validation_date"
  final val SOURCE = "source"
  final val TABLE_NAME = "table_name"
  final val GROUP_NAME = "group_name"
  final val IS_VALID = "is_valid"
  final val MESSAGE = "message"
  final val TIMESTAMP = "timestamp"
}
