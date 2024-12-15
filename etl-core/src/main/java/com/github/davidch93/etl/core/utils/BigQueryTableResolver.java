package com.github.davidch93.etl.core.utils;

import com.github.davidch93.etl.core.constants.Source;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves and parses BigQuery table strings into meaningful components.
 * <p>
 * A BigQuery table string is expected to follow the format:
 * <code>dataset.tableName</code>.
 * </p>
 * <p>
 * Example table: <code>my_dataset.my_table</code>
 * <ul>
 *     <li><b>dataset:</b> Represents the BigQuery dataset name and contains the source system.</li>
 *     <li><b>tableName:</b> Represents the table name within the dataset.</li>
 * </ul>
 *
 * @author david.christianto
 */
public class BigQueryTableResolver {

    private static final Pattern BIGQUERY_TABLE_PATTERN = Pattern.compile("^(?<dataset>[^.]+)\\.(?<tableName>[^.]+)$");

    private final String dataset;
    private final String tableName;

    private BigQueryTableResolver(String dataset, String tableName) {
        this.dataset = dataset;
        this.tableName = tableName;
    }

    /**
     * Parses a BigQuery table string and creates a {@link BigQueryTableResolver} object with extracted details.
     *
     * @param bigQueryTable the BigQuery table string (e.g., "my_dataset.my_table").
     * @return a {@link BigQueryTableResolver} object containing parsed details.
     * @throws IllegalArgumentException if the table string does not match the expected pattern.
     */
    public static BigQueryTableResolver resolve(String bigQueryTable) {
        Matcher matcher = BIGQUERY_TABLE_PATTERN.matcher(bigQueryTable);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid BigQuery table format: `" + bigQueryTable + "`!");
        }

        return new BigQueryTableResolver(
            matcher.group("dataset"),
            matcher.group("tableName")
        );
    }

    /**
     * Determines the source system based on the dataset name.
     *
     * @return the corresponding {@link Source} enum value.
     * @throws IllegalArgumentException if the source system cannot be determined.
     */
    public Source identifySource() {
        if (dataset.contains("mysql")) {
            return Source.MYSQL;
        } else if (dataset.contains("postgresql")) {
            return Source.POSTGRESQL;
        } else if (dataset.contains("mongodb")) {
            return Source.MONGODB;
        } else if (dataset.contains("dynamodb")) {
            return Source.DYNAMODB;
        } else if (dataset.contains("warehouse")) {
            return Source.DATA_WAREHOUSE;
        } else if (dataset.contains("mart")) {
            return Source.DATA_MART;
        } else {
            throw new IllegalArgumentException("Unable to determine source for dataset: `" + dataset + "`!");
        }
    }

    /**
     * Gets the table name.
     *
     * @return the table name.
     */
    public String getTableName() {
        return tableName;
    }
}
