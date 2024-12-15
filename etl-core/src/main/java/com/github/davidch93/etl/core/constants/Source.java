package com.github.davidch93.etl.core.constants;

/**
 * Enumeration representing the supported types of any sources.
 * <p>
 * This enum is primarily used to categorize and identify the sources
 * supported by the ETL (Extract, Transform, Load) process.
 * </p>
 *
 * <ul>
 *     <li>{@link #MYSQL} - Represents a MySQL database.</li>
 *     <li>{@link #POSTGRESQL} - Represents a PostgreSQL database.</li>
 *     <li>{@link #MONGODB} - Represents a MongoDB database.</li>
 *     <li>{@link #DYNAMODB} - Represents a DynamoDB database.</li>
 *     <li>{@link #DATA_WAREHOUSE} - Represents a Data Warehouse system.</li>
 *     <li>{@link #DATA_MART} - Represents a Data Mart system.</li>
 * </ul>
 *
 * @author david.christianto
 */
public enum Source {
    MYSQL,
    POSTGRESQL,
    MONGODB,
    DYNAMODB,

    DATA_WAREHOUSE,
    DATA_MART,
}
