package com.github.davidch93.etl.stream.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * An interface class to manage parameters/options for the data pool stream pipeline.
 *
 * @author david.christianto
 */
public interface StreamOptions extends DataflowPipelineOptions {

    @Description("The stream pipeline type")
    @Validation.Required
    PipelineType getPipelineType();

    void setPipelineType(PipelineType pipelineType);

    @Description("The configuration path in GCS")
    @Validation.Required
    String getConfigPath();

    void setConfigPath(String configPath);
}
