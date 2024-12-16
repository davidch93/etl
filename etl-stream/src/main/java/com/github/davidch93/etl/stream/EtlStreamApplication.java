package com.github.davidch93.etl.stream;

import com.github.davidch93.etl.stream.options.StreamCLI;
import com.github.davidch93.etl.stream.options.StreamOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * The {@code EtlStreamApplication} serves as the main entry point for the ETL Stream Pipeline.
 * <p>
 * This class initializes and starts the pipeline by:
 * <ul>
 *   <li>Parses command-line arguments into {@link StreamOptions} using {@link PipelineOptionsFactory}.</li>
 *   <li>Validates the parsed options for correctness.</li>
 *   <li>Creates and executes the pipeline by delegating to {@link StreamCLI}</li>
 * </ul>
 *
 * <p>
 * The application leverages Apache Beam for stream processing and expects configuration
 * to be passed as command-line arguments.
 * </p>
 *
 * @author david.christianto
 */
public class EtlStreamApplication {

    /**
     * Starts the ETL stream pipeline.
     *
     * @param args the input arguments for configuring the pipeline. These should match
     *             the options defined in {@link StreamOptions}.
     */
    public static void main(String... args) {
        StreamOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(StreamOptions.class);

        StreamCLI.withOptions(options).run();
    }
}
