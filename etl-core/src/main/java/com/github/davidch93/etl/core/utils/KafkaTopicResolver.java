package com.github.davidch93.etl.core.utils;

import com.github.davidch93.etl.core.constants.Source;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves and parses Kafka topic strings into meaningful components and determines the source system.
 * <p>
 * A Kafka topic string is expected to follow the format:
 * <code>clusterName.databaseName.tableName</code>.
 * </p>
 * <p>
 * Example topic: <code>mysqlstaging.github_staging.orders</code>
 * <ul>
 *     <li><b>clusterName:</b> Represents the CDC cluster name and contains the source system.</li>
 *     <li><b>databaseName:</b> Represents the database name within the cluster.</li>
 *     <li><b>tableName:</b> Represents the table name within the database.</li>
 * </ul>
 *
 * @author david.christianto
 */
public class KafkaTopicResolver {

    private static final Pattern TOPIC_PATTERN = Pattern.compile("^(?<clusterName>[^.]+)\\.(?<databaseName>[^.]+)\\.(?<tableName>[^.]+)$");

    private final String clusterName;
    private final String databaseName;
    private final String tableName;

    private KafkaTopicResolver(String clusterName, String databaseName, String tableName) {
        this.clusterName = clusterName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    /**
     * Parses a Kafka topic string and creates a {@link KafkaTopicResolver} object with extracted details.
     *
     * @param kafkaTopic the Kafka topic string (e.g., "mysqlstaging.db1.table1").
     * @return a {@link KafkaTopicResolver} object containing parsed details.
     * @throws IllegalArgumentException if the topic string does not match the expected pattern.
     */
    public static KafkaTopicResolver resolve(String kafkaTopic) {
        Matcher matcher = TOPIC_PATTERN.matcher(kafkaTopic);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid Kafka topic format: `" + kafkaTopic + "`!");
        }

        return new KafkaTopicResolver(
            matcher.group("clusterName"),
            matcher.group("databaseName"),
            matcher.group("tableName")
        );
    }

    /**
     * Determines the source system based on the cluster name.
     *
     * @return the corresponding {@link Source} enum value.
     * @throws IllegalArgumentException if the source system cannot be determined.
     */
    public Source identifySource() {
        if (clusterName.contains("mysql")) {
            return Source.MYSQL;
        } else if (clusterName.contains("postgresql")) {
            return Source.POSTGRESQL;
        } else if (clusterName.contains("mongodb")) {
            return Source.MONGODB;
        } else if (clusterName.contains("dynamodb")) {
            return Source.DYNAMODB;
        } else {
            throw new IllegalArgumentException("Unable to determine source for cluster name: `" + clusterName + "`!");
        }
    }

    /**
     * Gets the database name.
     *
     * @return the database name.
     */
    public String getDatabaseName() {
        return databaseName;
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
