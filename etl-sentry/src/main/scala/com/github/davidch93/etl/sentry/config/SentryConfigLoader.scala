package com.github.davidch93.etl.sentry.config

import com.github.davidch93.etl.core.config.ConfigLoader

/**
 * Utility object for loading a [[SentryConfiguration]] from a Filesystem.
 *
 * `SentryConfigLoader` provides methods to parse and load configuration.
 * It extends the functionality of `ConfigLoader` to deserialize configurations
 * from a provided file path  into a `SentryConfiguration` instance.
 *
 * @author david.christianto
 */
object SentryConfigLoader extends ConfigLoader {

  /**
   * Loads the `SentryConfiguration` from the specified configuration file path.
   *
   * @param configPath The file path to the configuration file in JSON format.
   *                   The configuration file must adhere to the structure defined
   *                   in the `SentryConfiguration` case class.
   * @return An instance of `SentryConfiguration` populated with values from the provided configuration file.
   * @throws RuntimeException if the configuration file is missing, invalid, or
   *                          does not match the expected structure.
   */
  def fromConfigPath(configPath: String): SentryConfiguration = {
    loadConfig(configPath, classOf[SentryConfiguration])
  }
}
