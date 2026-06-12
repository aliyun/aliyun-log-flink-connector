package com.aliyun.openservices.log.flink.sink;

import com.aliyun.openservices.aliyun.log.producer.Callback;
import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.flink.data.SinkRecord;
import com.aliyun.openservices.log.flink.model.AliyunLogSerializationSchema;
import com.aliyun.openservices.log.flink.util.ConfigParser;
import com.aliyun.openservices.log.flink.util.LogUtil;
import com.aliyun.openservices.log.http.signer.SignVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

/**
 * Sink writer backed by Aliyun Log Producer.
 */
public class AliyunLogSinkWriter<T> implements SinkWriter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AliyunLogSinkWriter.class);

    private final String project;
    private final String logstore;
    private final String endpoint;
    private final String accessKeyId;
    private final String accessKey;
    private final AliyunLogSerializationSchema<T> schema;
    private final AtomicLong pendingRequests = new AtomicLong(0);
    private final AtomicReference<IOException> asyncFailure = new AtomicReference<>();
    private final Producer producer;
    private final Callback callback;
    private final Collector<SinkRecord> collector;

    AliyunLogSinkWriter(
            String project,
            String logstore,
            String endpoint,
            String accessKeyId,
            String accessKey,
            Properties properties,
            AliyunLogSerializationSchema<T> schema) {
        this.project = project;
        this.logstore = logstore;
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKey = accessKey;
        this.schema = schema;
        this.producer = createProducer(properties);
        this.callback = new ProducerCallback();
        this.collector = new ProducerCollector();
        LOG.info("Created AliyunLogSinkWriter for project={}, logstore={}", project, logstore);
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkAsyncFailure();
        try {
            schema.serialize(element, collector);
        } catch (SinkWriteException e) {
            throw e.getCause();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to serialize sink record", e);
        }
        checkAsyncFailure();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        long sleepTime = 10L;
        while (pendingRequests.get() > 0) {
            checkAsyncFailure();
            Thread.sleep(sleepTime);
            sleepTime = Math.min(sleepTime * 2L, 100L);
        }
        checkAsyncFailure();
    }

    @Override
    public void close() throws Exception {
        try {
            flush(true);
        } finally {
            producer.close();
        }
        LOG.info("Closed AliyunLogSinkWriter for project={}, logstore={}", project, logstore);
    }

    private Producer createProducer(Properties properties) {
        ConfigParser parser = new ConfigParser(properties);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setLingerMs(parser.getInt(FLUSH_INTERVAL_MS, ProducerConfig.DEFAULT_LINGER_MS));
        producerConfig.setRetries(parser.getInt(MAX_RETRIES, ProducerConfig.DEFAULT_RETRIES));
        producerConfig.setBaseRetryBackoffMs(
                parser.getLong(BASE_RETRY_BACK_OFF_TIME_MS, ProducerConfig.DEFAULT_BASE_RETRY_BACKOFF_MS));
        producerConfig.setMaxRetryBackoffMs(
                parser.getLong(MAX_RETRY_BACK_OFF_TIME_MS, ProducerConfig.DEFAULT_MAX_RETRY_BACKOFF_MS));
        producerConfig.setMaxBlockMs(parser.getLong(MAX_BLOCK_TIME_MS, ProducerConfig.DEFAULT_MAX_BLOCK_MS));
        producerConfig.setIoThreadCount(parser.getInt(IO_THREAD_NUM, ProducerConfig.DEFAULT_IO_THREAD_COUNT));
        producerConfig.setBuckets(parser.getInt(BUCKETS, ProducerConfig.DEFAULT_BUCKETS));
        producerConfig.setTotalSizeInBytes(
                parser.getInt(TOTAL_SIZE_IN_BYTES, ProducerConfig.DEFAULT_TOTAL_SIZE_IN_BYTES));
        producerConfig.setAdjustShardHash(parser.getBool(PRODUCER_ADJUST_SHARD_HASH, true));

        SignVersion signVersion = LogUtil.parseSignVersion(parser.getString(SIGNATURE_VERSION));
        if (signVersion == SignVersion.V4) {
            String regionId = parser.getString(REGION_ID);
            if (StringUtils.isBlank(regionId)) {
                throw new IllegalArgumentException(
                        "The " + REGION_ID + " was not specified for signature " + signVersion.name() + ".");
            }
            producerConfig.setRegion(regionId);
            producerConfig.setSignVersion(com.aliyun.openservices.log.http.signer.SignVersion.V4);
        } else {
            producerConfig.setSignVersion(com.aliyun.openservices.log.http.signer.SignVersion.V1);
        }

        Producer newProducer = new LogProducer(producerConfig);
        newProducer.putProjectConfig(new ProjectConfig(project, endpoint, accessKeyId, accessKey));
        return newProducer;
    }

    private void send(SinkRecord record) throws IOException, InterruptedException {
        if (record == null) {
            return;
        }
        LogItem logItem = record.getLogItem();
        if (logItem == null) {
            LOG.warn("Skipping SinkRecord with null LogItem: {}", record);
            return;
        }
        String targetLogstore = StringUtils.isBlank(record.getLogstore()) ? logstore : record.getLogstore();
        pendingRequests.incrementAndGet();
        try {
            producer.send(
                    project,
                    targetLogstore,
                    record.getTopic(),
                    record.getSource(),
                    record.getHashKey(),
                    logItem,
                    callback);
        } catch (ProducerException e) {
            pendingRequests.decrementAndGet();
            throw new IOException("Failed to send log record", e);
        } catch (InterruptedException e) {
            pendingRequests.decrementAndGet();
            throw e;
        }
    }

    private void checkAsyncFailure() throws IOException {
        IOException failure = asyncFailure.get();
        if (failure != null) {
            throw failure;
        }
    }

    private class ProducerCollector implements Collector<SinkRecord> {
        @Override
        public void collect(SinkRecord record) {
            try {
                send(record);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while sending log record", e);
            } catch (IOException e) {
                throw new SinkWriteException(e);
            }
        }

        @Override
        public void close() {
        }
    }

    private class ProducerCallback implements Callback {
        @Override
        public void onCompletion(Result result) {
            if (result == null) {
                asyncFailure.compareAndSet(null, new IOException("SLS producer returned null result"));
            } else if (!result.isSuccessful()) {
                asyncFailure.compareAndSet(
                        null,
                        new IOException(
                                "Failed to send log: errorCode=" + result.getErrorCode()
                                        + ", errorMessage=" + result.getErrorMessage()
                                        + ", attempts=" + result.getAttemptCount()));
            }
            pendingRequests.decrementAndGet();
        }
    }

    private static class SinkWriteException extends RuntimeException {
        private SinkWriteException(IOException cause) {
            super(cause);
        }

        @Override
        public IOException getCause() {
            return (IOException) super.getCause();
        }
    }
}
