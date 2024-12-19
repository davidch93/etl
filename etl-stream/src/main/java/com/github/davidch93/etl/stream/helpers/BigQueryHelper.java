package com.github.davidch93.etl.stream.helpers;

import com.github.davidch93.etl.core.config.BigQueryConfig;
import com.github.davidch93.etl.core.constants.Dataset;
import com.github.davidch93.etl.core.constants.Source;
import com.github.davidch93.etl.core.schema.Field.FieldType;
import com.github.davidch93.etl.core.schema.Table;
import com.google.api.services.bigquery.model.*;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.davidch93.etl.core.constants.MetadataField.IS_DELETED;
import static com.github.davidch93.etl.core.constants.MetadataField.TS_PARTITION;

/**
 * Helper class for interacting with Google BigQuery.
 *
 * <p>This class provides methods to manage datasets, create and modify tables, create views,
 * and execute SQL queries in the context of an ETL pipeline. It abstracts interactions
 * with BigQuery APIs and integrates schema changes based on source and table configurations.</p>
 *
 * <p>The helper also supports different dataset types (e.g., daily, stream, real-time)
 * and handles metadata fields based on the source type (e.g., MySQL, PostgreSQL, MongoDB).</p>
 *
 * @author david.christianto
 */
public class BigQueryHelper {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryHelper.class);

    private static final String DAY_PARTITIONING = "DAY";
    private static final int MAX_RETRIES = 10;

    private final BigQueryServices bigQueryServices;
    private final DatasetService datasetService;
    private final JobService jobService;
    private final BigQueryConfig bigQueryConfig;

    /**
     * Constructor a BigQueryHelper with default {@link BigQueryServices}.
     *
     * @param options        the BigQuery options.
     * @param bigQueryConfig the BigQuery configuration.
     */
    public BigQueryHelper(BigQueryOptions options, BigQueryConfig bigQueryConfig) {
        this.bigQueryServices = new BigQueryServicesImpl();
        this.datasetService = bigQueryServices.getDatasetService(options);
        this.jobService = bigQueryServices.getJobService(options);
        this.bigQueryConfig = bigQueryConfig;
    }

    /**
     * Constructor a BigQueryHelper for custom {@link BigQueryServices}.
     *
     * @param bigQueryServices custom BigQuery services.
     * @param options          the BigQuery options.
     * @param bigQueryConfig   the BigQuery configuration.
     */
    public BigQueryHelper(BigQueryServices bigQueryServices, BigQueryOptions options, BigQueryConfig bigQueryConfig) {
        this.bigQueryServices = bigQueryServices;
        this.datasetService = bigQueryServices.getDatasetService(options);
        this.jobService = bigQueryServices.getJobService(options);
        this.bigQueryConfig = bigQueryConfig;
    }

    /**
     * Prepares stream tables and real-time views by creating or updating schemas and views as necessary.
     *
     * @param tables the list of tables to process.
     * @throws RuntimeException If an error occurs while preparing the stream tables and real-time views.
     */
    public void prepareStreamTablesAndRealTimeViews(Collection<Table> tables) {
        tables.forEach(table -> {
            com.google.api.services.bigquery.model.Table currentStreamTable = getStreamTable(table);
            TableSchema streamSchema = buildTableSchema(Dataset.STREAM, table);
            TableSchema realTimeSchema = buildTableSchema(Dataset.REAL_TIME, table);

            if (currentStreamTable == null) {
                createStreamTable(table, streamSchema);
                createOrReplaceRealTimeView(table, realTimeSchema);
            } else {
                Map<String, String> latestFields = toFieldMap(streamSchema.getFields());
                Map<String, String> currentFields = toFieldMap(currentStreamTable.getSchema().getFields());

                if (!latestFields.keySet().equals(currentFields.keySet())) {
                    alterStreamTable(table, latestFields, currentFields);
                    createOrReplaceRealTimeView(table, realTimeSchema);
                }
            }
        });
    }

    /**
     * Retrieves the stream table metadata for the specified table.
     *
     * @param table the {@link Table} object representing the table whose metadata is to be retrieved.
     * @return the table object containing the table metadata or {@code null} if the table does not exist.
     * @throws RuntimeException if there is an error while retrieving the table metadata.
     */
    private com.google.api.services.bigquery.model.Table getStreamTable(Table table) {
        try {
            TableReference tableReference = new TableReference()
                .setProjectId(bigQueryConfig.getProjectId())
                .setDatasetId(bigQueryConfig.getDatasetId(Dataset.STREAM, table.getSource()))
                .setTableId(table.getName());

            return datasetService.getTable(tableReference);
        } catch (InterruptedException | IOException ex) {
            throw new RuntimeException("Failed to retrieve table `" + table.getName() + "`!", ex);
        }
    }

    /**
     * Constructs the schema for a table based on dataset type and table configuration following these steps.
     * <ol>
     *   <li>Construct fields based on the table schema</li>
     *   <li>Add a partition column (from a daily table) to the list of fields if it exists.</li>
     *   <li>Add metadata columns specific to the database type.</li>
     *   <li>Convert the list of fields to a BigQuery TableSchema.</li>
     * </ol>
     *
     * @param dataset the {@link Dataset} indicating the type of dataset (e.g., STREAM, DAILY, REAL_TIME).
     * @param table   the {@link Table} object representing the table whose schema is to be generated.
     * @return the constructed table schema.
     */
    private TableSchema buildTableSchema(Dataset dataset, Table table) {
        List<TableFieldSchema> fields = table.getSchema().getFields().stream()
            .map(field -> new TableFieldSchema()
                .setName(field.getName())
                .setType(field.getType() == FieldType.DOUBLE ? "FLOAT" : field.getType().toString())
                .setMode(field.isNullable() ? "NULLABLE" : "REQUIRED")
                .setDescription(field.getDescription()))
            .collect(Collectors.toList());

        table.getTablePartition().ifPresent(tablePartition ->
            fields.add(new TableFieldSchema()
                .setName(tablePartition.getPartitionColumn())
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
                .setDescription(tablePartition.getDescription()))
        );

        if (dataset == Dataset.STREAM) {
            fields.addAll(getMetadataFieldsForSource(table.getSource()));
        } else {
            fields.add(new TableFieldSchema()
                .setName(IS_DELETED)
                .setType("BOOLEAN")
                .setMode("REQUIRED")
                .setDescription("Indicates if the record is marked as deleted."));
        }

        return new TableSchema().setFields(fields);
    }

    /**
     * Retrieves the metadata fields to be included in the schema based on the source system.
     *
     * @param source the {@link Source} system of the table (e.g., MYSQL, POSTGRESQL, MONGODB).
     * @return a {@link List} of {@link TableFieldSchema} objects representing the metadata fields.
     * @throws IllegalArgumentException if the source type is unsupported.
     */
    private List<TableFieldSchema> getMetadataFieldsForSource(Source source) {
        return switch (source) {
            case MYSQL -> Query.MYSQL_METADATA_FIELDS;
            case POSTGRESQL -> Query.POSTGRESQL_METADATA_FIELDS;
            case MONGODB -> Query.MONGODB_METADATA_FIELDS;
            default -> throw new IllegalArgumentException("Unsupported source type for `" + source + "`!");
        };
    }

    /**
     * Creates a new stream table in BigQuery with the specified schema.
     *
     * @param table       the {@link Table} object representing the table to be created.
     * @param tableSchema the {@link TableSchema} object defining the schema of the table.
     * @throws RuntimeException if there is an error while creating the table.
     */
    private void createStreamTable(Table table, TableSchema tableSchema) {
        try {
            com.google.api.services.bigquery.model.Table bgTable = new com.google.api.services.bigquery.model.Table()
                .setTableReference(new TableReference()
                    .setProjectId(bigQueryConfig.getProjectId())
                    .setDatasetId(bigQueryConfig.getDatasetId(Dataset.STREAM, table.getSource()))
                    .setTableId(table.getName()))
                .setSchema(tableSchema)
                .setTimePartitioning(new TimePartitioning()
                    .setField(TS_PARTITION)
                    .setType(DAY_PARTITIONING)
                    .setExpirationMs(bigQueryConfig.getPartitionExpiryMillis()))
                .setDescription(table.getSchema().getDescription());

            logger.info("[ETL-STREAM] Creating the stream table for `{}`.", table.getName());
            datasetService.createTable(bgTable);
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException("Failed to create a stream table for `" + table.getName() + "`!");
        }
    }

    /**
     * Alters the schema of an existing stream table in BigQuery to match the latest schema definition.
     * <p>
     * This method compares the fields of the latest schema with the fields of the current schema and generates
     * SQL queries to add or drop columns as necessary to align the current schema with the latest schema.
     * The SQL queries are then executed to alter the BigQuery table accordingly.
     * </p>
     *
     * <p><b>Note:</b>
     * The BigQuery {@link TableFieldSchema#setType(String)} and {@link TableFieldSchema#setDescription(String)}
     * are not supported in the Legacy SQL query.
     * </p>
     *
     * @param table         the {@link Table} object representing the table to be altered.
     * @param latestFields  a {@link Map} containing the latest field names and types.
     * @param currentFields a {@link Map} containing the current field names and types.
     */
    private void alterStreamTable(Table table, Map<String, String> latestFields, Map<String, String> currentFields) {
        String streamTableName = bigQueryConfig.getFullyQualifiedTableName(Dataset.STREAM, table.getSource(), table.getName());

        List<String> queries = new LinkedList<>();
        latestFields.forEach((name, type) -> {
            if (!currentFields.containsKey(name)) {
                queries.add(Query.addColumn(name, type));
            }
        });
        currentFields.keySet().forEach(name -> {
            if (!latestFields.containsKey(name)) {
                queries.add(Query.dropColumn(name));
            }
        });

        logger.info("[ETL-STREAM] Altering the stream table for `{}`. Queries: {}.",
            table.getName(), String.join(", ", queries));
        executeJobQuery(Query.alterTable(streamTableName, String.join(", ", queries)));
    }

    /**
     * Creates or replaces a real-time view in BigQuery based on the specified table and schema.
     * <p>
     * This method creates a real-time view table that merges stream data and daily data.
     * The SQL queries are then executed to create or replace the BigQuery view table accordingly.
     *
     * @param table       the {@link Table} object representing the table for which the view is created.
     * @param tableSchema the {@link TableSchema} object defining the schema of the view.
     */
    private void createOrReplaceRealTimeView(Table table, TableSchema tableSchema) {
        String dailyTableName = bigQueryConfig.getFullyQualifiedTableName(Dataset.DAILY, table.getSource(), table.getName());
        String streamTableName = bigQueryConfig.getFullyQualifiedTableName(Dataset.STREAM, table.getSource(), table.getName());
        String realTimeTableName = bigQueryConfig.getFullyQualifiedTableName(Dataset.REAL_TIME, table.getSource(), table.getName());

        String selectPattern = tableSchema.getFields().stream()
            .map(TableFieldSchema::getName)
            .collect(Collectors.joining("`, `"));

        String partitionPattern = String.join("`, `", table.getConstraintKeys());

        String joinPattern = table.getConstraintKeys().stream()
            .map(Query::joinTable)
            .collect(Collectors.joining(" AND "));

        String wherePattern = table.getConstraintKeys().stream()
            .map(Query::whereFilter)
            .collect(Collectors.joining(" AND "));

        String aggregatePattern = tableSchema.getFields().stream()
            .filter(field -> !table.getConstraintKeys().contains(field.getName()))
            .map(field -> Query.arrayAgg(field.getName()))
            .collect(Collectors.joining(",\n"));

        switch (table.getSource()) {
            case MYSQL:
                executeJobQuery(Query.mysqlView(dailyTableName, streamTableName, realTimeTableName, selectPattern,
                    partitionPattern, joinPattern, wherePattern));
                break;
            case POSTGRESQL:
                executeJobQuery(Query.postgresView(dailyTableName, streamTableName, realTimeTableName, selectPattern,
                    partitionPattern, joinPattern, wherePattern));
                break;
            case MONGODB:
                executeJobQuery(Query.mongoView(dailyTableName, streamTableName, realTimeTableName, selectPattern,
                    partitionPattern, aggregatePattern));
                break;
        }
    }

    /**
     * Executes a query on BigQuery using the specified SQL string
     * and wait until it's DONE (whether it's succeed or not) or exceed the {@link #MAX_RETRIES}.
     *
     * @param query the SQL query string to execute.
     * @throws RuntimeException if the query execution fails or encounters errors.
     */
    private void executeJobQuery(String query) {
        try {
            JobReference jobReference = new JobReference()
                .setProjectId(bigQueryConfig.getProjectId())
                .setLocation(bigQueryConfig.getRegion())
                .setJobId(UUID.randomUUID().toString().replaceAll("-", ""));
            JobConfigurationQuery jobConfigurationQuery = new JobConfigurationQuery()
                .setQuery(query)
                .setUseLegacySql(false);

            jobService.startQueryJob(jobReference, jobConfigurationQuery);

            Job job = jobService.pollJob(jobReference, MAX_RETRIES);
            if (job.getStatus() == null) {
                throw new RuntimeException("Query execution failed after %d retries!".formatted(MAX_RETRIES));
            } else if (job.getStatus().getErrorResult() != null) {
                throw new RuntimeException("Unable to execute query, got error :" + job.getStatus().getErrorResult().getMessage());
            } else if (job.getStatus().getErrors() != null) {
                throw new RuntimeException("Job Submitted but got error : " + job.getStatus().getErrors());
            }
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException("Failed to execute query `" + query + "`!", ex);
        }
    }

    /**
     * Converts a list of {@link TableFieldSchema} objects into a {@link Map} where the key is the field name
     * and the value is the field type. Maintains the insertion order of the fields.
     *
     * <p>If duplicate field names are encountered, the last occurrence will overwrite the previous one.</p>
     *
     * @param fields the list of {@link TableFieldSchema} objects to be converted
     * @return a {@link Map} with field names as keys and field types as values, preserving the insertion order
     * @throws NullPointerException if the {@code fields} list or any of its elements is {@code null}
     */
    private Map<String, String> toFieldMap(List<TableFieldSchema> fields) {
        return fields.stream()
            .collect(Collectors.toMap(
                TableFieldSchema::getName,
                TableFieldSchema::getType,
                (existing, replacement) -> replacement,
                LinkedHashMap::new
            ));
    }
}
