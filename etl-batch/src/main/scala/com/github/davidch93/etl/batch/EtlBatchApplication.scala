package com.github.davidch93.etl.batch

import com.github.davidch93.etl.batch.cli.BatchCLI

/**
 * The `EtlBatchApplication` serves as the entry point for the ETL Batch application.
 *
 * <h3>Responsibilities:</h3>
 *  - Initializes the application by invoking the `BatchCLI`, which handles command-line
 *    arguments and orchestrates the execution of appropriate ETL pipelines.
 *
 * <h3>Execution Flow:</h3>
 *  1. Parses the command-line arguments provided by the user.
 *  1. Determines the command to execute (`create_data_pool`, etc.) and runs the corresponding ETL pipeline.
 *
 * <h3>Supported Commands:</h3>
 *  - `create_data_pool`: Runs the data pool pipeline.
 *
 * @author david.christianto
 */
object EtlBatchApplication {

  /**
   * The main method serves as the application entry point.
   *
   * @param args Command-line arguments passed during application execution.
   *             These arguments are forwarded to the `BatchCLI` for parsing and execution.
   */
  def main(args: Array[String]): Unit = {
    new BatchCLI().parse(args).run()
  }
}
