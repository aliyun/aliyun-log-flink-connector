package com.aliyun.openservices.log.flink.source;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.source.deserialization.AliyunLogDeserializationSchema;
import com.aliyun.openservices.log.flink.source.enumerator.AliyunLogSplitAssigner;

import java.util.Properties;

/**
 * Builder for creating AliyunLogSource instances.
 */
public class AliyunLogSourceBuilder<T> {
    private String project;
    private String logstore;
    private AliyunLogDeserializationSchema<T> deserializer;
    private Properties configProps = new Properties();
    private AliyunLogSplitAssigner splitAssigner;
    private String accessKeyId;
    private String accessKey;

    /**
     * Set the Aliyun Log Service project name.
     *
     * @param project the project name
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setProject(String project) {
        this.project = project;
        return this;
    }

    /**
     * Set the LogStore to consume from.
     *
     * @param logstore the logstore name
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setLogStore(String logstore) {
        this.logstore = logstore;
        return this;
    }

    /**
     * Set the endpoint for Aliyun Log Service.
     *
     * @param endpoint the endpoint URL
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setEndpoint(String endpoint) {
        configProps.setProperty(ConfigConstants.LOG_ENDPOINT, endpoint);
        return this;
    }

    /**
     * Set both access key ID and access key (convenience method).
     *
     * @param accessKeyId the access key ID
     * @param accessKey  the access key secret
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setCredentials(String accessKeyId, String accessKey) {
        this.accessKeyId = accessKeyId;
        this.accessKey = accessKey;
        return this;
    }

    /**
     * Set the consumer group name.
     *
     * @param consumerGroup the consumer group name
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setConsumerGroup(String consumerGroup) {
        configProps.setProperty(ConfigConstants.LOG_CONSUMERGROUP, consumerGroup);
        return this;
    }

    /**
     * Set the deserialization schema using AliyunLogDeserializationSchema.
     *
     * @param deserializer the deserialization schema
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setDeserializer(AliyunLogDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Set the beginning position for consumption using StartingPosition enum.
     *
     * @param startingPosition the starting position enum value
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setStartingPosition(StartingPosition startingPosition) {
        configProps.setProperty(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, startingPosition.getValue());
        return this;
    }

    /**
     * Set the beginning position for consumption using string.
     * Valid values: "earliest", "latest", "checkpoint", "timestamp", or a unix timestamp.
     * Legacy values: "begin_cursor", "end_cursor", "consumer_from_checkpoint" are also supported.
     *
     * @param startingPosition the starting position string
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setStartingPosition(String startingPosition) {
        configProps.setProperty(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, startingPosition);
        return this;
    }

    /**
     * Set the fallback position to use when no checkpoint is restored.
     * This position will be used if there is no checkpoint available from previous runs.
     *
     * @param fallbackPosition the starting position to use when no checkpoint exists
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setFallbackPosition(StartingPosition fallbackPosition) {
        configProps.setProperty(ConfigConstants.LOG_CONSUMER_DEFAULT_POSITION, fallbackPosition.getValue());
        return this;
    }

    /**
     * Set the split assigner for custom split (shard) assignment logic.
     * <p>
     * The split assigner determines how splits (shards) are distributed among
     * parallel readers. If not specified, {@link com.aliyun.openservices.log.flink.source.enumerator.assigner.ModuloSplitAssigner}
     * is used as the default.
     * <p>
     * <b>Available implementations:</b>
     * <ul>
     *   <li><b>{@link com.aliyun.openservices.log.flink.source.enumerator.assigner.ModuloSplitAssigner}</b>
     *       (default): Deterministic assignment based on shard ID modulo.
     *       Same shard always goes to the same reader. Best for stateful processing
     *       or when split affinity matters.</li>
     *   <li><b>{@link com.aliyun.openservices.log.flink.source.enumerator.assigner.RoundRobinSplitAssigner}</b>:
     *       Even distribution across all readers. Ensures balanced load.
     *       Best when all splits have similar processing requirements.</li>
     * </ul>
     * <p>
     * <b>Examples:</b>
     * <pre>{@code
     * // Use round-robin for balanced distribution
     * .setSplitAssigner(new RoundRobinSplitAssigner())
     *
     * // Use modulo for deterministic assignment (default)
     * .setSplitAssigner(new ModuloSplitAssigner())
     *
     * // Use custom assigner for domain-specific logic
     * .setSplitAssigner(new CustomSplitAssigner())
     * }</pre>
     *
     * @param splitAssigner the split assigner to use for distributing splits
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setSplitAssigner(AliyunLogSplitAssigner splitAssigner) {
        this.splitAssigner = splitAssigner;
        return this;
    }

    /**
     * Set a custom property.
     *
     * @param key   the property key
     * @param value the property value
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setProperty(String key, String value) {
        configProps.setProperty(key, value);
        return this;
    }

    /**
     * Set all properties from a Properties object.
     *
     * @param properties the properties to set
     * @return this builder for method chaining
     */
    public AliyunLogSourceBuilder<T> setProperties(Properties properties) {
        this.configProps.putAll(properties);
        return this;
    }

    /**
     * Build the AliyunLogSource instance.
     *
     * @return the configured AliyunLogSource instance
     * @throws IllegalArgumentException if required fields are not set
     */
    public AliyunLogSource<T> build() {
        if (project == null || project.isEmpty()) {
            throw new IllegalArgumentException("Project must be set");
        }
        if (logstore == null || logstore.isEmpty()) {
            throw new IllegalArgumentException("LogStore must be set");
        }
        if (deserializer == null) {
            throw new IllegalArgumentException("Deserializer must be set");
        }
        if (configProps.getProperty(ConfigConstants.LOG_ENDPOINT) == null) {
            throw new IllegalArgumentException("Endpoint must be set");
        }
        if (accessKeyId == null || accessKeyId.isEmpty()) {
            throw new IllegalArgumentException("AccessKeyId must be set");
        }
        if (accessKey == null || accessKey.isEmpty()) {
            throw new IllegalArgumentException("AccessKey must be set");
        }
        return new AliyunLogSource<>(
                project, logstore, deserializer,
                configProps, splitAssigner, accessKeyId, accessKey);
    }
}

