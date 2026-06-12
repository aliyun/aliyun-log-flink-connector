package com.aliyun.openservices.log.flink.source.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Options for the Aliyun Log SQL connector.
 */
public final class AliyunLogConnectorOptions {
    public static final String IDENTIFIER = "aliyun-log";

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun Log Service endpoint.");

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun Log Service project.");

    public static final ConfigOption<String> LOGSTORE =
            ConfigOptions.key("logstore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun Log Service logstore.");

    public static final ConfigOption<String> ACCESS_KEY_ID =
            ConfigOptions.key("access-key-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun access key id.");

    public static final ConfigOption<String> ACCESS_KEY =
            ConfigOptions.key("access-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun access key secret.");

    public static final ConfigOption<String> CONSUMER_GROUP =
            ConfigOptions.key("consumer-group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Consumer group used for checkpoint commits.");

    public static final ConfigOption<String> BEGIN_POSITION =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("earliest")
                    .withDescription("Startup position: earliest, latest, checkpoint, or a unix timestamp.");

    public static final ConfigOption<String> DEFAULT_POSITION =
            ConfigOptions.key("scan.startup.default-position")
                    .stringType()
                    .defaultValue("earliest")
                    .withDescription("Fallback position when startup mode is checkpoint and no checkpoint exists.");

    public static final ConfigOption<String> CHECKPOINT_MODE =
            ConfigOptions.key("checkpoint.mode")
                    .stringType()
                    .defaultValue("on-checkpoints")
                    .withDescription("Checkpoint commit mode: on-checkpoints, periodic, or disabled.");

    public static final ConfigOption<Long> COMMIT_INTERVAL =
            ConfigOptions.key("commit.interval.ms")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Periodic checkpoint commit interval in milliseconds.");

    public static final ConfigOption<Long> FETCH_INTERVAL =
            ConfigOptions.key("fetch.interval.ms")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Minimum fetch interval in milliseconds when no data is returned.");

    public static final ConfigOption<Integer> MAX_NUMBER_PER_FETCH =
            ConfigOptions.key("max.number.per.fetch")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Maximum log groups fetched per request.");

    public static final ConfigOption<Long> SHARDS_DISCOVERY_INTERVAL =
            ConfigOptions.key("shards.discovery.interval.ms")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Shard discovery interval in milliseconds.");

    public static final ConfigOption<String> STOP_TIME =
            ConfigOptions.key("stop.time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional stop time as unix timestamp.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("If true, invalid field values are emitted as null.");

    public static final ConfigOption<String> REGION_ID =
            ConfigOptions.key("region.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Region id for signature v4.");

    public static final ConfigOption<String> SIGNATURE_VERSION =
            ConfigOptions.key("signature.version")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Signature version, V1 or V4.");

    private AliyunLogConnectorOptions() {
    }
}
