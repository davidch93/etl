package com.github.davidch93.etl.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigLoaderTest {

    static class TestConfig {
        @JsonProperty("group_name")
        private String groupName;

        @JsonProperty("max_threads")
        private int maxThreads;

        @JsonProperty("kafka_config")
        private KafkaConfig kafkaConfig;

        @JsonProperty("bigquery_config")
        private BigQueryConfig bigQueryConfig;

        public String getGroupName() {
            return groupName;
        }

        public int getMaxThreads() {
            return maxThreads;
        }

        public KafkaConfig getKafkaConfig() {
            return kafkaConfig;
        }

        public BigQueryConfig getBigQueryConfig() {
            return bigQueryConfig;
        }
    }

    static class TestConfigLoader extends ConfigLoader {

        public TestConfig fromConfigPath(String configPath) {
            return loadConfig(configPath, TestConfig.class);
        }

        public static TestConfig loadConfig(String configPath) {
            return new TestConfigLoader().fromConfigPath(configPath);
        }
    }

    @Test
    void testLoadConfig() {
        String configPath = "src/test/resources/config/etl-config.json";
        TestConfig config = TestConfigLoader.loadConfig(configPath);

        assertThat(config.getGroupName()).isEqualTo("non-financial");
        assertThat(config.getMaxThreads()).isEqualTo(30);

        KafkaConfig kafkaConfig = config.getKafkaConfig();
        assertThat(kafkaConfig).isNotNull();
        assertThat(kafkaConfig.getBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(kafkaConfig.getGroupId()).isEqualTo("etl.staging");
        assertThat(kafkaConfig.getDefaultOffset()).isEqualTo("earliest");

        BigQueryConfig bigQueryConfig = config.getBigQueryConfig();
        assertThat(bigQueryConfig).isNotNull();
        assertThat(bigQueryConfig.getProjectId()).isEqualTo("github-staging");
        assertThat(bigQueryConfig.getRegion()).isEqualTo("asia-southeast1");
        assertThat(bigQueryConfig.getDatasetId()).isEqualTo("bronze");
        assertThat(bigQueryConfig.getCreateDisposition()).isEqualTo("CREATE_IF_NEEDED");
        assertThat(bigQueryConfig.getPartitionExpiryMillis()).isEqualTo(172800000L);
        assertThat(bigQueryConfig.getTemporaryGcsBucket()).isEqualTo("gs://dataproc-temp-staging");
    }

    @Test
    void testLoadConfig_withInvalidConfigFile() {
        String invalidPath = "src/test/resources/config/invalid-config.json";

        assertThatThrownBy(() -> TestConfigLoader.loadConfig(invalidPath))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to load configuration from `" + invalidPath + "`!");
    }

    @Test
    void testLoadConfig_withMalformedJson() throws IOException {
        Path malformedPath = Paths.get("src/test/resources/config/malformed-config.json");
        Files.write(malformedPath, "{group_name: non-financial, max_threads:}".getBytes(StandardCharsets.UTF_8));

        try {
            assertThatThrownBy(() -> TestConfigLoader.loadConfig(malformedPath.toString()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to map JSON to object from input stream!");
        } finally {
            Files.deleteIfExists(malformedPath);
        }
    }
}