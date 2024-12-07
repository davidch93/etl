package com.github.davidch93.etl.core.constants;

/**
 * Enumeration representing the supported types of databases.
 * <p>
 * This enum is primarily used to categorize and identify the database
 * systems supported by the ETL (Extract, Transform, Load) process.
 * </p>
 *
 * <ul>
 *     <li>{@link #MYSQL} - Represents a MySQL database.</li>
 *     <li>{@link #POSTGRESQL} - Represents a PostgreSQL database.</li>
 *     <li>{@link #MONGODB} - Represents a MongoDB database.</li>
 *     <li>{@link #DYNAMODB} - Represents a DynamoDB database.</li>
 * </ul>
 *
 * @author david.christianto
 */
public enum DatabaseType {
    MYSQL,
    POSTGRESQL,
    MONGODB,
    DYNAMODB
}
