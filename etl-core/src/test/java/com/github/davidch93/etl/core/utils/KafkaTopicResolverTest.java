package com.github.davidch93.etl.core.utils;

import com.github.davidch93.etl.core.constants.Source;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KafkaTopicResolverTest {

    @Test
    void testResolve_withMySqlKafkaTopic_shouldResolveParameters() {
        String topic = "mysqlstaging.github_staging.orders";
        KafkaTopicResolver resolver = KafkaTopicResolver.resolve(topic);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.MYSQL);
        assertThat(resolver.getDatabaseName()).isEqualTo("github_staging");
        assertThat(resolver.getTableName()).isEqualTo("orders");
    }

    @Test
    void testResolve_withPostgreSqlKafkaTopic_shouldResolveParameters() {
        String topic = "postgresqlstaging.github_staging.users";
        KafkaTopicResolver resolver = KafkaTopicResolver.resolve(topic);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.POSTGRESQL);
        assertThat(resolver.getDatabaseName()).isEqualTo("github_staging");
        assertThat(resolver.getTableName()).isEqualTo("users");
    }

    @Test
    void testResolve_withMongoDbKafkaTopic_shouldResolveParameters() {
        String topic = "mongodbstaging.github_staging.transactions";
        KafkaTopicResolver resolver = KafkaTopicResolver.resolve(topic);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.MONGODB);
        assertThat(resolver.getDatabaseName()).isEqualTo("github_staging");
        assertThat(resolver.getTableName()).isEqualTo("transactions");
    }

    @Test
    void testResolve_withDynamoDbKafkaTopic_shouldResolveParameters() {
        String topic = "dynamodbstaging.github_staging.transactions";
        KafkaTopicResolver resolver = KafkaTopicResolver.resolve(topic);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.DYNAMODB);
        assertThat(resolver.getDatabaseName()).isEqualTo("github_staging");
        assertThat(resolver.getTableName()).isEqualTo("transactions");
    }

    @Test
    void testResolve_shouldThrowExceptionForInvalidKafkaTopic() {
        String invalidTopic = "invalid_topic";

        assertThatThrownBy(() -> KafkaTopicResolver.resolve(invalidTopic))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid Kafka topic format: `invalid_topic`!");
    }

    @Test
    void testResolve_shouldThrowExceptionForUnknownSource() {
        String unknownSourceTopic = "unknown_staging.github_staging.orders";
        KafkaTopicResolver resolver = KafkaTopicResolver.resolve(unknownSourceTopic);

        assertThatThrownBy(resolver::identifySource)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unable to determine source for cluster name: `unknown_staging`!");
    }
}
