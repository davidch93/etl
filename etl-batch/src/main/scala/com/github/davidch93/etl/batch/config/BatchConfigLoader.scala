package com.github.davidch93.etl.batch.config

import com.github.davidch93.etl.core.config.ConfigLoader

/**
 * A utility object for loading the batch ETL configuration from a specified file path.
 *
 * It leverages the `ConfigLoader` base class to read and parse the configuration into
 * a `BatchConfiguration` instance.
 *
 * @author david.christianto
 */
object BatchConfigLoader extends ConfigLoader {

  /**
   * Loads the batch configuration from the specified file path.
   *
   * @param configPath The file path to the JSON configuration file.
   * @return An instance of `BatchConfiguration` populated with values from the provided configuration file.
   * @throws RuntimeException if the configuration file cannot be loaded or parsed
   */
  def fromConfigPath(configPath: String): BatchConfiguration = {
    loadConfig(configPath, classOf[BatchConfiguration])
  }
}
