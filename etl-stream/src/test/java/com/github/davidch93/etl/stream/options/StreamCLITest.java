package com.github.davidch93.etl.stream.options;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StreamCLITest {

    @Test
    void testRun_withUnsupportedPipelineType_thenExpectThrowException() {
        String[] args = new String[]{
            "--pipelineType=CUSTOM",
            "--configPath=gs://github-config-staging/config/github-etl-stream-staging.json"
        };

        StreamOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(StreamOptions.class);

        assertThatThrownBy(() -> StreamCLI.withOptions(options).run())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported pipeline type for `CUSTOM`!");
    }
}
