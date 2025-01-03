package com.github.davidch93.etl.stream.transformers;

import com.github.davidch93.etl.core.config.BigQueryConfig;
import com.github.davidch93.etl.core.constants.Dataset;
import com.github.davidch93.etl.core.constants.Source;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import java.util.Objects;

import static com.github.davidch93.etl.core.constants.MetadataField.SOURCE;
import static com.github.davidch93.etl.core.constants.MetadataField.TABLE_NAME;

/**
 * A class responsible for generating {@link TableDestination} objects
 * based on the provided {@link BigQueryConfig}.
 * <p>
 * This class implements {@link SerializableFunction} to allow its instances to be used as functions
 * in Apache Beam transforms. It maps {@link TableRow} objects to {@link TableDestination} objects,
 * considering the configuration provided by {@link BigQueryConfig}.
 * </p>
 *
 * <p><strong>Note:</strong> Instances of this class are intended to be used with Apache Beam pipelines.</p>
 *
 * @author david.christianto
 */
public class TableDescriptor implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {

    private final BigQueryConfig bigQueryConfig;

    /**
     * Constructs a new {@code TableDescriptor} instance with the specified {@link BigQueryConfig}.
     *
     * @param bigQueryConfig the BigQuery configuration.
     */
    private TableDescriptor(BigQueryConfig bigQueryConfig) {
        this.bigQueryConfig = bigQueryConfig;
    }

    /**
     * Creates a new {@code TableDescriptor} instance with the specified {@link BigQueryConfig}.
     *
     * @param bigQueryConfig the BigQuery configuration.
     * @return a new {@code TableDescriptor} instance.
     */
    public static TableDescriptor describe(BigQueryConfig bigQueryConfig) {
        return new TableDescriptor(bigQueryConfig);
    }

    /**
     * Applies the transformation to map a {@link TableRow} to a {@link TableDestination}.
     *
     * @param input the input {@link ValueInSingleWindow} containing the {@link TableRow} to be transformed.
     * @return the resulting {@link TableDestination}.
     * @throws NullPointerException if the input {@link TableRow} is null.
     */
    @Override
    public TableDestination apply(ValueInSingleWindow<TableRow> input) {
        TableRow row = Objects.requireNonNull(Objects.requireNonNull(input).getValue());
        Source source = Source.valueOf(row.get(SOURCE).toString());
        TableReference tableReference = new TableReference()
            .setProjectId(bigQueryConfig.getProjectId())
            .setDatasetId(bigQueryConfig.getDatasetId(Dataset.STREAM, source))
            .setTableId(row.get(TABLE_NAME).toString());

        return new TableDestination(tableReference, "");
    }
}
