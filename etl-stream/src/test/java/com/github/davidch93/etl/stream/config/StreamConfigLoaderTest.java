package com.github.davidch93.etl.stream.config;

import com.github.davidch93.etl.core.config.BigQueryConfig;
import com.github.davidch93.etl.core.config.KafkaConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.io.ByteStreams;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StreamConfigLoaderTest {

    private static Storage localStorage;

    @BeforeAll
    static void setup() throws IOException {
        localStorage = LocalStorageHelper.getOptions().getService();

        String bucket = "github-config-staging";
        String filePath = "config/github-etl-stream-staging.json";
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, filePath)).build();

        try (InputStream inputStream = StreamConfigLoaderTest.class.getClassLoader().getResourceAsStream(filePath)) {
            byte[] bytes = ByteStreams.toByteArray(Objects.requireNonNull(inputStream));
            localStorage.create(blobInfo, bytes);
        }
    }

    @Test
    void testFromConfigPath_whenFileExists_thenExpectValidConfig() {
        String configPath = "gs://github-config-staging/config/github-etl-stream-staging.json";
        StreamConfiguration config = StreamConfigLoader.load(localStorage).fromConfigPath(configPath);

        assertThat(config.getConfigBucket()).isEqualTo("gs://github-config-staging");
        assertThat(config.getTableWhitelist()).containsAnyOf(
            "mysqlstaging.github_staging.orders",
            "postgresqlstaging.github_staging.users",
            "mongodbstaging.github_staging.transactions"
        );

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
        assertThat(bigQueryConfig.getTemporaryGcsBucket()).isEqualTo("gs://dataflow-temp-staging");
    }

    @Test
    void testFromConfigPath_withInvalidPattern_expectThrowException() {
        String invalidPath = "config/github-etl-stream-staging.json";

        assertThatThrownBy(() -> StreamConfigLoader.load(localStorage).fromConfigPath(invalidPath))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Config path does not match the GCS patter: " + invalidPath + "!");
    }

    @Test
    void testFromConfigPath_whenConfigNotExist_expectThrowException() {
        String configPath = "gs://github-config-staging/config/github-etl-stream-preproduction.json";

        assertThatThrownBy(() -> StreamConfigLoader.load(localStorage).fromConfigPath(configPath))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to read config from `" + configPath + "`!");
    }
}
