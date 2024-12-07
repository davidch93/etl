package com.github.davidch93.etl.core.schema;

import com.github.davidch93.etl.core.utils.JsonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utility class responsible for loading table schemas from a specified path.
 * <p>
 * This class provides a method to read and deserialize table schema definitions
 * from JSON files stored in a filesystem.
 *
 * @author david.christianto
 */
public final class SchemaLoader {

    private static final Logger logger = LoggerFactory.getLogger(SchemaLoader.class);

    /**
     * Loads and deserializes a table schema from the specified path.
     *
     * @param schemaFilePath the filesystem path to the schema file.
     * @return the {@link Table} object representing the schema definition.
     * @throws RuntimeException if an I/O error occurs while reading or deserializing the schema file.
     */
    public static Table loadTableSchema(String schemaFilePath) {
        logger.info("[ETL-CORE] Loading schema from `{}`.", schemaFilePath);

        try {
            Path path = new Path(schemaFilePath);
            FileSystem fileSystem = path.getFileSystem(new Configuration());

            try (InputStream inputStream = fileSystem.open(path)) {
                Table table = JsonUtils.readValue(inputStream, Table.class);
                logger.info("[ETL-CORE] Loaded table schema: {}.", table);
                return table;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load schema from `" + schemaFilePath + "`!", e);
        }
    }
}
