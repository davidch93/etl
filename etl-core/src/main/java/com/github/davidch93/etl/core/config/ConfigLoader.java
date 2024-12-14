package com.github.davidch93.etl.core.config;

import com.github.davidch93.etl.core.utils.JsonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract class for loading job configuration from a filesystem path and deserializing it into a specified class type.
 *
 * <p>
 * Provides a common mechanism to read a configuration file from a given path and convert it into an instance
 * of a specified configuration class using Jackson's ObjectMapper.
 * </p>
 *
 * <p>
 * Subclasses should extend this abstract class to implement specific job-related configuration loading logic.
 * </p>
 *
 * <p><strong>Note:</strong>
 * The configuration file must be in JSON format and should match the structure of the specified configuration class.
 * </p>
 *
 * @author david.christianto
 */
public abstract class ConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    /**
     * Loads a configuration file from the given filesystem path and converts it into the specified class type.
     *
     * @param configPath  the location of the configuration file in the filesystem.
     * @param configClass the class type of the configuration to deserialize.
     * @param <T>         the type of the configuration class.
     * @return the deserialized configuration object.
     * @throws RuntimeException if an I/O error occurs while interacting with the file system or reading the file.
     */
    protected <T> T loadConfig(String configPath, Class<T> configClass) {
        logger.info("[ETL-CORE] Loading configuration from `{}`.", configPath);

        try {
            Path path = new Path(configPath);
            FileSystem fileSystem = path.getFileSystem(new Configuration());

            try (InputStream inputStream = fileSystem.open(path)) {
                T config = JsonUtils.readValue(inputStream, configClass);
                logger.info("[ETL-CORE] Loaded configuration: {}", config);
                return config;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration from `" + configPath + "`!", e);
        }
    }
}