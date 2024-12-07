package com.github.davidch93.etl.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * A configuration class to store information about Kafka consumer properties.
 *
 * <p>
 * This class provides a structured way to configure essential Kafka consumer properties required
 * for interacting with a Kafka cluster. The properties include the bootstrap servers,
 * consumer group ID, and the default offset for Kafka operations.
 * </p>
 *
 * <p>
 * Instances of this class are typically loaded from a configuration file and passed to
 * components that need to interact with Kafka. The class is designed to be compatible
 * with Jackson for JSON serialization and deserialization.
 * </p>
 *
 * <p><strong>Note:</strong> Default values are assigned to certain fields, such as
 * the `defaultOffset` field, which defaults to "latest" if not explicitly set.
 * </p>
 *
 * @author david.christianto
 */
public class KafkaConfig implements Serializable {

    @JsonProperty(value = "bootstrap_servers", required = true)
    private String bootstrapServers;

    @JsonProperty(value = "group_id")
    private String groupId;

    @JsonProperty(value = "default_offset")
    private String defaultOffset = "latest";

    /**
     * Gets the bootstrap servers associated with the Kafka configuration.
     *
     * @return The bootstrap servers.
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Gets the group ID associated with the Kafka configuration.
     *
     * @return The group ID.
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * Gets the default offset used in Kafka operations.
     *
     * @return The default offset.
     */
    public String getDefaultOffset() {
        return defaultOffset;
    }
}
