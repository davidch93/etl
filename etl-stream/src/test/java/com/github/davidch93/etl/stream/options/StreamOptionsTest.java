package com.github.davidch93.etl.stream.options;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StreamOptionsTest {

    @Test
    void testLoadOptions_withValidArgs_thenExpectValidOptions() {
        String configPath = "gs://github-config-staging/config/github-etl-stream-staging.json";
        String[] args = new String[]{
            "--pipelineType=DATA_POOL_STREAM",
            "--configPath=" + configPath
        };

        StreamOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(StreamOptions.class);
        assertThat(options.getPipelineType()).isEqualTo(PipelineType.DATA_POOL_STREAM);
        assertThat(options.getConfigPath()).isEqualTo(configPath);
    }

    @Test
    void testLoadOptions_withMissingArgs_thenExpectThrowException() {
        String[] args = new String[]{
            "--configPath=gs://github-config-staging/config/github-etl-stream-staging.json"
        };

        assertThatThrownBy(() -> PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamOptions.class))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required value for [--pipelineType, \"The stream pipeline type\"].");
    }
}
