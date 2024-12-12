package com.github.davidch93.etl.stream.options;

import com.github.davidch93.etl.core.pipelines.EtlPipeline;
import com.github.davidch93.etl.stream.pipelines.DataPoolStreamPipeline;

/**
 * A command-line interface (CLI) for running different types of stream pipelines.
 * This class provides a convenient way to run various stream pipelines based on the provided options.
 *
 * <p>
 * Example usage:
 * <pre>{@code
 * StreamOptions options = ...; // Options for configuring the stream pipeline
 * StreamCLI.withOptions(options).run();
 * }</pre>
 * </p>
 *
 * <p>
 * To run other types of pipelines, additional cases can be added to the switch statement in the {@link #run()} method.
 * </p>
 *
 * <p>
 * Note: This CLI assumes that the provided options specify a valid pipeline type.
 * If an unsupported pipeline type is provided, an {@code IllegalArgumentException} will be thrown.
 * </p>
 *
 * @author david.christianto
 */
public class StreamCLI {

    private final StreamOptions streamOptions;

    private StreamCLI(StreamOptions streamOptions) {
        this.streamOptions = streamOptions;
    }

    /**
     * Creates a new instance of {@code StreamCLI} with the given stream options.
     *
     * @param streamOptions The options for configuring the stream pipeline.
     * @return The {@code StreamCLI} instance.
     */
    public static StreamCLI withOptions(StreamOptions streamOptions) {
        return new StreamCLI(streamOptions);
    }

    /**
     * Runs the stream pipeline based on the provided options.
     * This method initializes and starts the appropriate stream pipeline
     * based on the pipeline type specified in the options.
     *
     * @throws IllegalArgumentException If an unsupported pipeline type is provided.
     */
    public void run() {
        EtlPipeline etlPipeline = switch (streamOptions.getPipelineType()) {
            case DATA_POOL_STREAM -> new DataPoolStreamPipeline(streamOptions);
            default ->
                throw new IllegalArgumentException("Unsupported pipeline type for `" + streamOptions.getPipelineType() + "`!");
        };

        etlPipeline.prepareAndStart();
    }
}
