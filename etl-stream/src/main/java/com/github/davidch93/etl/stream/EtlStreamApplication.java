package com.github.davidch93.etl.stream;

import com.github.davidch93.etl.stream.options.StreamCLI;
import com.github.davidch93.etl.stream.options.StreamOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * The main entrypoint of the stream pipeline.
 *
 * @author david.christianto
 */
public class EtlStreamApplication {

    /**
     * Start the stream pipeline.
     *
     * @param args the input arguments
     */
    public static void main(String... args) {
        StreamOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(StreamOptions.class);

        StreamCLI.withOptions(options).run();
    }
}
