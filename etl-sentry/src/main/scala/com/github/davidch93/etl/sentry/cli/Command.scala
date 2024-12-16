package com.github.davidch93.etl.sentry.cli

/**
 * The `Command` object defines the command names used in the ETL Sentry CLI application.
 * These commands determine the type of pipeline to execute, such as validation or constraint suggestion.
 *
 * @author david.christianto
 */
object Command {
  final val VALIDATION_CMD = "validate"
  final val CONSTRAINT_SUGGESTION_CMD = "suggest_constraint_rules"
}
