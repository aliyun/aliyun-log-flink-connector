package com.aliyun.openservices.log.flink.source.table;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.ReflectiveLogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.StaticCredentialsProviderFactory;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Factory for discovering the Aliyun Log SQL connector.
 */
public class AliyunLogDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return AliyunLogConnectorOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(Arrays.asList(
                AliyunLogConnectorOptions.ENDPOINT,
                AliyunLogConnectorOptions.PROJECT,
                AliyunLogConnectorOptions.LOGSTORE));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(Arrays.asList(
                FactoryUtil.SOURCE_PARALLELISM,
                AliyunLogConnectorOptions.ACCESS_KEY_ID,
                AliyunLogConnectorOptions.ACCESS_KEY,
                AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_FACTORY_CLASS,
                AliyunLogConnectorOptions.CONSUMER_GROUP,
                AliyunLogConnectorOptions.BEGIN_POSITION,
                AliyunLogConnectorOptions.DEFAULT_POSITION,
                AliyunLogConnectorOptions.CHECKPOINT_MODE,
                AliyunLogConnectorOptions.COMMIT_INTERVAL,
                AliyunLogConnectorOptions.FETCH_INTERVAL,
                AliyunLogConnectorOptions.MAX_NUMBER_PER_FETCH,
                AliyunLogConnectorOptions.SHARDS_DISCOVERY_INTERVAL,
                AliyunLogConnectorOptions.STOP_TIME,
                AliyunLogConnectorOptions.IGNORE_PARSE_ERRORS,
                AliyunLogConnectorOptions.REGION_ID,
                AliyunLogConnectorOptions.SIGNATURE_VERSION,
                AliyunLogConnectorOptions.SINK_TOPIC,
                AliyunLogConnectorOptions.SINK_SOURCE,
                AliyunLogConnectorOptions.SINK_PARALLELISM,
                AliyunLogConnectorOptions.FLUSH_INTERVAL,
                AliyunLogConnectorOptions.MAX_RETRIES,
                AliyunLogConnectorOptions.BASE_RETRY_BACKOFF,
                AliyunLogConnectorOptions.MAX_RETRY_BACKOFF,
                AliyunLogConnectorOptions.MAX_BLOCK_TIME,
                AliyunLogConnectorOptions.IO_THREAD_NUM,
                AliyunLogConnectorOptions.PRODUCER_BUCKETS,
                AliyunLogConnectorOptions.TOTAL_SIZE_IN_BYTES,
                AliyunLogConnectorOptions.PRODUCER_ADJUST_SHARD_HASH));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_PARAMETER_PREFIX);

        ReadableConfig options = helper.getOptions();
        String project = options.get(AliyunLogConnectorOptions.PROJECT);
        String logstore = options.get(AliyunLogConnectorOptions.LOGSTORE);
        String endpoint = options.get(AliyunLogConnectorOptions.ENDPOINT);
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();
        LogCredentialsProviderFactory credentialsProviderFactory =
                createCredentialsProviderFactory(catalogOptions);
        RowType rowType = (RowType) context.getCatalogTable()
                .getResolvedSchema()
                .toPhysicalRowDataType()
                .getLogicalType();

        Properties properties = new Properties();
        properties.setProperty(ConfigConstants.LOG_PROJECT, project);
        properties.setProperty(ConfigConstants.LOG_LOGSTORE, logstore);
        properties.setProperty(ConfigConstants.LOG_ENDPOINT, endpoint);
        putOptional(properties, ConfigConstants.LOG_CONSUMERGROUP, options, AliyunLogConnectorOptions.CONSUMER_GROUP);
        putOptional(properties, ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, options, AliyunLogConnectorOptions.BEGIN_POSITION);
        putOptional(properties, ConfigConstants.LOG_CONSUMER_DEFAULT_POSITION, options, AliyunLogConnectorOptions.DEFAULT_POSITION);
        putOptional(properties, ConfigConstants.LOG_CHECKPOINT_MODE, options, AliyunLogConnectorOptions.CHECKPOINT_MODE);
        putOptional(properties, ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS, options, AliyunLogConnectorOptions.COMMIT_INTERVAL);
        putOptional(properties, ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, options, AliyunLogConnectorOptions.FETCH_INTERVAL);
        putOptional(properties, ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, options, AliyunLogConnectorOptions.MAX_NUMBER_PER_FETCH);
        putOptional(properties, ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS, options, AliyunLogConnectorOptions.SHARDS_DISCOVERY_INTERVAL);
        putOptional(properties, ConfigConstants.STOP_TIME, options, AliyunLogConnectorOptions.STOP_TIME);
        putOptional(properties, ConfigConstants.REGION_ID, options, AliyunLogConnectorOptions.REGION_ID);
        putOptional(properties, ConfigConstants.SIGNATURE_VERSION, options, AliyunLogConnectorOptions.SIGNATURE_VERSION);

        if (hasDynamicCredentials(catalogOptions)) {
            return new AliyunLogDynamicSource(
                    project,
                    logstore,
                    endpoint,
                    credentialsProviderFactory,
                    properties,
                    rowType,
                    options.get(AliyunLogConnectorOptions.IGNORE_PARSE_ERRORS),
                    options.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null));
        }
        return new AliyunLogDynamicSource(
                project,
                logstore,
                endpoint,
                options.get(AliyunLogConnectorOptions.ACCESS_KEY_ID),
                options.get(AliyunLogConnectorOptions.ACCESS_KEY),
                properties,
                rowType,
                options.get(AliyunLogConnectorOptions.IGNORE_PARSE_ERRORS),
                options.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_PARAMETER_PREFIX);

        ReadableConfig options = helper.getOptions();
        String project = options.get(AliyunLogConnectorOptions.PROJECT);
        String logstore = options.get(AliyunLogConnectorOptions.LOGSTORE);
        String endpoint = options.get(AliyunLogConnectorOptions.ENDPOINT);
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();
        LogCredentialsProviderFactory credentialsProviderFactory =
                createCredentialsProviderFactory(catalogOptions);
        RowType rowType = (RowType) context.getCatalogTable()
                .getResolvedSchema()
                .toPhysicalRowDataType()
                .getLogicalType();

        Properties properties = new Properties();
        properties.setProperty(ConfigConstants.LOG_PROJECT, project);
        properties.setProperty(ConfigConstants.LOG_LOGSTORE, logstore);
        properties.setProperty(ConfigConstants.LOG_ENDPOINT, endpoint);
        putOptional(properties, ConfigConstants.REGION_ID, options, AliyunLogConnectorOptions.REGION_ID);
        putOptional(properties, ConfigConstants.SIGNATURE_VERSION, options, AliyunLogConnectorOptions.SIGNATURE_VERSION);
        putOptional(properties, ConfigConstants.FLUSH_INTERVAL_MS, options, AliyunLogConnectorOptions.FLUSH_INTERVAL);
        putOptional(properties, ConfigConstants.MAX_RETRIES, options, AliyunLogConnectorOptions.MAX_RETRIES);
        putOptional(properties, ConfigConstants.BASE_RETRY_BACK_OFF_TIME_MS, options, AliyunLogConnectorOptions.BASE_RETRY_BACKOFF);
        putOptional(properties, ConfigConstants.MAX_RETRY_BACK_OFF_TIME_MS, options, AliyunLogConnectorOptions.MAX_RETRY_BACKOFF);
        putOptional(properties, ConfigConstants.MAX_BLOCK_TIME_MS, options, AliyunLogConnectorOptions.MAX_BLOCK_TIME);
        putOptional(properties, ConfigConstants.IO_THREAD_NUM, options, AliyunLogConnectorOptions.IO_THREAD_NUM);
        putOptional(properties, ConfigConstants.BUCKETS, options, AliyunLogConnectorOptions.PRODUCER_BUCKETS);
        putOptional(properties, ConfigConstants.TOTAL_SIZE_IN_BYTES, options, AliyunLogConnectorOptions.TOTAL_SIZE_IN_BYTES);
        putOptional(properties, ConfigConstants.PRODUCER_ADJUST_SHARD_HASH, options, AliyunLogConnectorOptions.PRODUCER_ADJUST_SHARD_HASH);

        if (hasDynamicCredentials(catalogOptions)) {
            return new AliyunLogDynamicSink(
                    project,
                    logstore,
                    endpoint,
                    credentialsProviderFactory,
                    properties,
                    rowType,
                    options.get(AliyunLogConnectorOptions.SINK_TOPIC),
                    options.getOptional(AliyunLogConnectorOptions.SINK_SOURCE).orElse(null),
                    options.getOptional(AliyunLogConnectorOptions.SINK_PARALLELISM).orElse(null));
        }
        return new AliyunLogDynamicSink(
                project,
                logstore,
                endpoint,
                options.get(AliyunLogConnectorOptions.ACCESS_KEY_ID),
                options.get(AliyunLogConnectorOptions.ACCESS_KEY),
                properties,
                rowType,
                options.get(AliyunLogConnectorOptions.SINK_TOPIC),
                options.getOptional(AliyunLogConnectorOptions.SINK_SOURCE).orElse(null),
                options.getOptional(AliyunLogConnectorOptions.SINK_PARALLELISM).orElse(null));
    }

    static LogCredentialsProviderFactory createCredentialsProviderFactory(
            Map<String, String> options) {
        Optional<String> accessKeyId = Optional.ofNullable(
                options.get(AliyunLogConnectorOptions.ACCESS_KEY_ID.key()));
        Optional<String> accessKey = Optional.ofNullable(
                options.get(AliyunLogConnectorOptions.ACCESS_KEY.key()));
        Optional<String> factoryClass = Optional.ofNullable(
                options.get(AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_FACTORY_CLASS.key()));

        boolean hasAccessKeyId = isPresent(accessKeyId);
        boolean hasAccessKey = isPresent(accessKey);
        boolean hasFactoryClass = isPresent(factoryClass);
        if (hasAccessKeyId != hasAccessKey) {
            throw new ValidationException(
                    "Options 'access.key.id' and 'access.key.secret' must be configured together");
        }
        if (hasFactoryClass == hasAccessKeyId) {
            throw new ValidationException(
                    "Configure exactly one credential mode: both static access key options, "
                            + "or 'credentials.provider.factory.class'");
        }
        Properties providerProperties = new Properties();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_PARAMETER_PREFIX)) {
                String providerKey = key.substring(
                        AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_PARAMETER_PREFIX.length());
                if (providerKey.isEmpty()) {
                    throw new ValidationException(
                            "Credentials provider parameter name must not be empty");
                }
                providerProperties.setProperty(providerKey, entry.getValue());
            }
        }
        if (hasAccessKeyId) {
            if (!providerProperties.isEmpty()) {
                throw new ValidationException(
                        "Options with prefix 'credentials.provider.param.' require "
                                + "'credentials.provider.factory.class'");
            }
            return new StaticCredentialsProviderFactory(accessKeyId.get(), accessKey.get());
        }
        return new ReflectiveLogCredentialsProviderFactory(
                factoryClass.get(),
                providerProperties);
    }

    private static boolean hasDynamicCredentials(Map<String, String> options) {
        return isPresent(Optional.ofNullable(
                options.get(AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_FACTORY_CLASS.key())));
    }

    private static boolean isPresent(Optional<String> value) {
        return value.isPresent() && !value.get().trim().isEmpty();
    }

    private static <T> void putOptional(
            Properties properties,
            String targetKey,
            ReadableConfig config,
            ConfigOption<T> option) {
        Optional<T> value = config.getOptional(option);
        value.ifPresent(v -> properties.setProperty(targetKey, String.valueOf(v)));
    }
}
