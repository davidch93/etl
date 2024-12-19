package com.github.davidch93.etl.stream.config;

import com.github.davidch93.etl.core.utils.JsonUtils;
import com.github.davidch93.etl.stream.helpers.GcsHelper;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility class for loading a {@link StreamConfiguration} from Google Cloud Storage (GCS).
 * <p>
 * Instances of this class can be created using the default {@code GcsHelper} or by providing a custom storage client.
 * </p>
 *
 * @author david.christianto
 */
public class StreamConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(StreamConfigLoader.class);
    private static final String GCS_REGEX = "gs://([^/]+)/(.+)";

    private final GcsHelper gcsHelper;

    /**
     * Constructs a new {@code StreamConfigLoader} instance using the default {@code GcsHelper}.
     */
    private StreamConfigLoader() {
        this.gcsHelper = new GcsHelper();
    }

    /**
     * Constructs a new {@code StreamConfigLoader} instance with the specified {@code GcsHelper}.
     *
     * @param storage the custom storage client.
     */
    private StreamConfigLoader(Storage storage) {
        this.gcsHelper = new GcsHelper(storage);
    }

    /**
     * Creates a new {@code StreamConfigLoader} instance using the default {@code GcsHelper}.
     *
     * @return the {@code StreamConfigLoader} instance.
     */
    public static StreamConfigLoader load() {
        return new StreamConfigLoader();
    }

    /**
     * Creates a new {@code StreamConfigLoader} instance with the specified {@code GcsHelper}.
     *
     * @param storage the custom storage client.
     * @return the {@code StreamConfigLoader} instance.
     */
    public static StreamConfigLoader load(Storage storage) {
        return new StreamConfigLoader(storage);
    }

    /**
     * Retrieves a {@link StreamConfiguration} from Google Cloud Storage (GCS) based on the provided path.
     * If the path matches the GCS pattern, the configuration is fetched from GCS.
     *
     * @param configPath the path to the configuration file in the format "gs://bucket-name/file-path".
     * @return the {@code StreamConfiguration} parsed from the specified file path or GCS.
     * @throws RuntimeException If there is an issue reading the configuration from the specified path.
     *                          This could occur if the path is invalid, the file is not found,
     *                          or an I/O error occurs during reading.
     */
    public StreamConfiguration fromConfigPath(String configPath) {
        logger.info("[ETL-STREAM] Loading config from `{}`.", configPath);
        try {
            Pattern pattern = Pattern.compile(GCS_REGEX);
            Matcher matcher = pattern.matcher(configPath);

            if (!matcher.matches()) {
                throw new RuntimeException("Config path does not match the GCS patter: " + configPath + "!");
            }

            String bucket = matcher.group(1);
            String filePath = matcher.group(2);
            String content = gcsHelper.readFile(bucket, filePath);

            StreamConfiguration config = JsonUtils.readValue(content, StreamConfiguration.class);
            logger.info("[DATAFLOW] Supplied configuration: {}", config);

            return config;
        } catch (IllegalArgumentException | StorageException ex) {
            throw new RuntimeException("Failed to read config from `" + configPath + "`!", ex);
        }
    }
}
