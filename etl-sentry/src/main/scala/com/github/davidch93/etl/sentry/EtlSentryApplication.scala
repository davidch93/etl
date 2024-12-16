package com.github.davidch93.etl.sentry

import com.github.davidch93.etl.sentry.cli.SentryCLI

/**
 * The `EtlSentryApplication` serves as the entry point for the ETL Sentry application.
 *
 * <h3>Responsibilities:</h3>
 *  - Initializes the application by invoking the `SentryCLI`, which handles command-line
 *    arguments and orchestrates the execution of appropriate ETL pipelines.
 *
 * <h3>Execution Flow:</h3>
 *  1. Parses the command-line arguments provided by the user.
 *  1. Determines the command to execute (`validate` or `suggest_constraint_rules`) and
 *     runs the corresponding ETL pipeline.
 *
 * <h3>Supported Commands:</h3>
 *  - `validate`: Runs the data validation pipeline.
 *  - `suggest_constraint_rules`: Runs the constraint suggestion pipeline.
 *
 * @author david.christianto
 */
object EtlSentryApplication {

  /**
   * The main method serves as the application entry point.
   *
   * @param args Command-line arguments passed during application execution.
   *             These arguments are forwarded to the `SentryCLI` for parsing and execution.
   */
  def main(args: Array[String]): Unit = {
    new SentryCLI().parse(args).run()
  }
}
