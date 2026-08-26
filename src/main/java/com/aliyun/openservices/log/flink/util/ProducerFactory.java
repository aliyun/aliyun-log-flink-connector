package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.http.signer.SignVersion;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

import static com.aliyun.openservices.log.flink.ConfigConstants.BASE_RETRY_BACK_OFF_TIME_MS;
import static com.aliyun.openservices.log.flink.ConfigConstants.BUCKETS;
import static com.aliyun.openservices.log.flink.ConfigConstants.FLUSH_INTERVAL_MS;
import static com.aliyun.openservices.log.flink.ConfigConstants.IO_THREAD_NUM;
import static com.aliyun.openservices.log.flink.ConfigConstants.MAX_BLOCK_TIME_MS;
import static com.aliyun.openservices.log.flink.ConfigConstants.MAX_RETRIES;
import static com.aliyun.openservices.log.flink.ConfigConstants.MAX_RETRY_BACK_OFF_TIME_MS;
import static com.aliyun.openservices.log.flink.ConfigConstants.PRODUCER_ADJUST_SHARD_HASH;
import static com.aliyun.openservices.log.flink.ConfigConstants.REGION_ID;
import static com.aliyun.openservices.log.flink.ConfigConstants.SIGNATURE_VERSION;
import static com.aliyun.openservices.log.flink.ConfigConstants.TOTAL_SIZE_IN_BYTES;

/** Creates SLS producers consistently for all Flink sink APIs. */
public final class ProducerFactory {

    private ProducerFactory() {
    }

    /**
     * Creates and initializes an SLS producer from connector properties and one credential mode.
     *
     * @param project SLS project
     * @param endpoint SLS endpoint
     * @param properties producer configuration properties
     * @param credentialsProviderFactory runtime credentials provider factory
     * @return initialized producer
     */
    public static Producer create(
            String project,
            String endpoint,
            Properties properties,
            LogCredentialsProviderFactory credentialsProviderFactory) {
        if (credentialsProviderFactory == null) {
            throw new IllegalArgumentException("CredentialsProviderFactory must not be null");
        }

        CredentialsProvider credentialsProvider =
                credentialsProviderFactory.createCredentialsProvider();
        if (credentialsProvider == null) {
            throw new IllegalStateException("CredentialsProviderFactory returned null");
        }
        ProjectConfig projectConfig = new ProjectConfig(
                project,
                endpoint,
                credentialsProvider,
                ProjectConfig.DEFAULT_USER_AGENT);

        Producer producer = new LogProducer(createProducerConfig(properties));
        try {
            producer.putProjectConfig(projectConfig);
            return producer;
        } catch (RuntimeException | Error e) {
            closeAfterInitializationFailure(producer, e);
            throw e;
        }
    }

    private static ProducerConfig createProducerConfig(Properties properties) {
        ConfigParser parser = new ConfigParser(properties);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setLingerMs(
                parser.getInt(FLUSH_INTERVAL_MS, ProducerConfig.DEFAULT_LINGER_MS));
        producerConfig.setRetries(
                parser.getInt(MAX_RETRIES, ProducerConfig.DEFAULT_RETRIES));
        producerConfig.setBaseRetryBackoffMs(parser.getLong(
                BASE_RETRY_BACK_OFF_TIME_MS,
                ProducerConfig.DEFAULT_BASE_RETRY_BACKOFF_MS));
        producerConfig.setMaxRetryBackoffMs(parser.getLong(
                MAX_RETRY_BACK_OFF_TIME_MS,
                ProducerConfig.DEFAULT_MAX_RETRY_BACKOFF_MS));
        producerConfig.setMaxBlockMs(
                parser.getLong(MAX_BLOCK_TIME_MS, ProducerConfig.DEFAULT_MAX_BLOCK_MS));
        producerConfig.setIoThreadCount(
                parser.getInt(IO_THREAD_NUM, ProducerConfig.DEFAULT_IO_THREAD_COUNT));
        producerConfig.setBuckets(
                parser.getInt(BUCKETS, ProducerConfig.DEFAULT_BUCKETS));
        producerConfig.setTotalSizeInBytes(parser.getInt(
                TOTAL_SIZE_IN_BYTES,
                ProducerConfig.DEFAULT_TOTAL_SIZE_IN_BYTES));
        producerConfig.setAdjustShardHash(
                parser.getBool(PRODUCER_ADJUST_SHARD_HASH, true));

        SignVersion signVersion = LogUtil.parseSignVersion(parser.getString(SIGNATURE_VERSION));
        if (signVersion == SignVersion.V4) {
            String regionId = parser.getString(REGION_ID);
            if (StringUtils.isBlank(regionId)) {
                throw new IllegalArgumentException(
                        "The " + REGION_ID + " was not specified for signature "
                                + signVersion.name() + ".");
            }
            producerConfig.setRegion(regionId);
            producerConfig.setSignVersion(
                    com.aliyun.openservices.log.http.signer.SignVersion.V4);
        } else {
            producerConfig.setSignVersion(
                    com.aliyun.openservices.log.http.signer.SignVersion.V1);
        }
        return producerConfig;
    }

    private static void closeAfterInitializationFailure(
            Producer producer,
            Throwable initializationFailure) {
        try {
            producer.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            initializationFailure.addSuppressed(e);
        } catch (ProducerException e) {
            initializationFailure.addSuppressed(e);
        }
    }
}
