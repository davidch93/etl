package com.github.davidch93.etl.core.constants;

/**
 * Enumeration representing the types of BigQuery datasets.
 * <p>
 * This enum is primarily used to categorize and identify the dataset
 * types supported by the ETL (Extract, Transform, Load) process.
 * </p>
 *
 * <ul>
 *     <li>{@link #DAILY} - Represents a daily dataset.</li>
 *     <li>{@link #STREAM} - Represents a streaming dataset.</li>
 *     <li>{@link #REAL_TIME} - Represents a real-time dataset.</li>
 * </ul>
 *
 * @author david.christianto
 */
public enum Dataset {
    DAILY,
    STREAM,
    REAL_TIME
}
