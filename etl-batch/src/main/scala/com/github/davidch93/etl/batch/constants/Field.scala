package com.github.davidch93.etl.batch.constants

/**
 * An object containing constants for tracking the status and details of ETL jobs.
 *
 * <strong>Note:</strong>
 * This object is immutable and all fields are declared as `final` for ease of use.
 *
 * @author david.christianto
 */
object Field {

  final val TABLE_NAME = "table_name"
  final val GROUP_NAME = "group_name"
  final val SCHEDULED_TIMESTAMP = "scheduled_timestamp"
  final val START_TIMESTAMP = "start_timestamp"
  final val END_TIMESTAMP = "end_timestamp"
  final val DURATION_SECONDS = "duration_seconds"
  final val IS_SUCCESS = "is_success"
  final val MESSAGE = "message"
}
