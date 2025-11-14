package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.aliyun.log.producer.*;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.flink.data.SinkRecord;
import com.aliyun.openservices.log.flink.model.LogSerializationSchemaV2;
import com.aliyun.openservices.log.flink.util.ConfigParser;
import com.aliyun.openservices.log.flink.util.LogUtil;
import com.aliyun.openservices.log.http.signer.SignVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static com.aliyun.openservices.log.flink.ConfigConstants.*;

public class FlinkLogProducerV2<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkLogProducerV2.class);
    private final LogSerializationSchemaV2<T> schema;
    private final AtomicLong buffered = new AtomicLong(0);
    private transient Producer producer;
    private transient ProducerCallback callback;
    private final String project;
    private final String logstore;
    private final ConfigParser configParser;
    private SinkCollector<T> sinkCollector;

    public FlinkLogProducerV2(final LogSerializationSchemaV2<T> schema, Properties configProps) {
        if (schema == null) {
            throw new IllegalArgumentException("schema cannot be null");
        }
        if (configProps == null) {
            throw new IllegalArgumentException("configProps cannot be null");
        }
        this.schema = schema;
        this.configParser = new ConfigParser(configProps);
        this.project = configParser.getString(ConfigConstants.LOG_PROJECT);
        this.logstore = configParser.getString(ConfigConstants.LOG_LOGSTORE);
    }

    private Producer createProducer(ConfigParser parser) {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setLingerMs(parser.getInt(FLUSH_INTERVAL_MS, ProducerConfig.DEFAULT_LINGER_MS));
        producerConfig.setRetries(parser.getInt(MAX_RETRIES, ProducerConfig.DEFAULT_RETRIES));
        producerConfig.setBaseRetryBackoffMs(
                parser.getLong(BASE_RETRY_BACK_OFF_TIME_MS, ProducerConfig.DEFAULT_BASE_RETRY_BACKOFF_MS));
        producerConfig.setMaxRetryBackoffMs(
                parser.getLong(MAX_RETRY_BACK_OFF_TIME_MS, ProducerConfig.DEFAULT_MAX_RETRY_BACKOFF_MS));
        producerConfig.setMaxBlockMs(
                parser.getLong(MAX_BLOCK_TIME_MS, ProducerConfig.DEFAULT_MAX_BLOCK_MS));
        producerConfig.setIoThreadCount(parser.getInt(IO_THREAD_NUM, ProducerConfig.DEFAULT_IO_THREAD_COUNT));
        producerConfig.setBuckets(parser.getInt(BUCKETS, ProducerConfig.DEFAULT_BUCKETS));
        producerConfig.setTotalSizeInBytes(parser.getInt(TOTAL_SIZE_IN_BYTES, ProducerConfig.DEFAULT_TOTAL_SIZE_IN_BYTES));
        producerConfig.setAdjustShardHash(parser.getBool(PRODUCER_ADJUST_SHARD_HASH, true));
        SignVersion signVersion = LogUtil.parseSignVersion(parser.getString(SIGNATURE_VERSION));
        if (signVersion == SignVersion.V4) {
            String regionId = parser.getString(REGION_ID);
            if (StringUtils.isBlank(regionId)) {
                throw new IllegalArgumentException("The " + REGION_ID + " was not specified for signature " + signVersion.name() + ".");
            }
            producerConfig.setRegion(regionId);
            producerConfig.setSignVersion(com.aliyun.openservices.log.http.signer.SignVersion.V4);
        } else {
            producerConfig.setSignVersion(com.aliyun.openservices.log.http.signer.SignVersion.V1);
        }
        Producer producer = new LogProducer(producerConfig);
        ProjectConfig config = new ProjectConfig(project,
                parser.getString(ConfigConstants.LOG_ENDPOINT),
                parser.getString(ConfigConstants.LOG_ACCESSKEYID),
                parser.getString(ConfigConstants.LOG_ACCESSKEY));
        producer.putProjectConfig(config);
        return producer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("Opening FlinkLogProducerV2 for project={}, logstore={}", project, logstore);
        if (callback == null) {
            callback = new ProducerCallback(buffered);
        }
        if (producer == null) {
            producer = createProducer(configParser);
            LOG.debug("Producer created successfully for project={}, logstore={}", project, logstore);
        }
        this.sinkCollector = new SinkCollector<>(buffered, producer, callback, project, logstore);
        LOG.info("FlinkLogProducerV2 opened successfully for project={}, logstore={}", project, logstore);
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (producer == null) {
            LOG.debug("Skipping snapshotState: producer is null");
            return;
        }
        long beginAt = System.currentTimeMillis();
        long sleepTime = 10;
        long maxSleepTime = 100;
        long checkpointId = context.getCheckpointId();
        LOG.debug("Starting snapshotState for checkpointId={}, initialBuffered={}", checkpointId, buffered.get());

        while (true) {
            long currentBuffered = buffered.get();
            if (currentBuffered <= 0) {
                long usedTime = System.currentTimeMillis() - beginAt;
                LOG.info("SnapshotState completed for checkpointId={}, usedTime={}ms, project={}, logstore={}",
                        checkpointId, usedTime, project, logstore);
                break;
            }
            Thread.sleep(sleepTime);
            sleepTime = Math.min(sleepTime * 2, maxSleepTime);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    }

    private static class SinkCollector<T> implements Collector<SinkRecord> {
        private final AtomicLong buffered;
        private Producer producer;
        private ProducerCallback callback;
        private String project;
        private String logstore;

        public SinkCollector(AtomicLong buffered, Producer producer, ProducerCallback callback, String project, String logstore) {
            this.buffered = buffered;
            this.producer = producer;
            this.callback = callback;
            this.project = project;
            this.logstore = logstore;
        }

        @Override
        public void collect(SinkRecord record) {
            if (record == null) {
                LOG.warn("Received null SinkRecord, skipping");
                return;
            }
            LogItem logItem = record.getLogItem();
            if (logItem == null) {
                LOG.warn("SinkRecord has null LogItem, skipping. project={}, logstore={}, topic={}",
                        project, logstore, record.getTopic());
                return;
            }
            String sinkLogStore = record.getLogstore() != null ? record.getLogstore() : logstore;
            try {
                producer.send(project,
                        sinkLogStore,
                        record.getTopic(),
                        record.getSource(),
                        record.getHashKey(),
                        logItem,
                        callback);
                buffered.incrementAndGet();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Sent log record to project={}, logstore={}, topic={}, buffered={}",
                            project, sinkLogStore, record.getTopic(), buffered.get());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted while sending log to project={}, logstore={}, topic={}, buffered={}",
                        project, sinkLogStore, record.getTopic(), buffered.get(), e);
                throw new RuntimeException("Interrupted while sending log", e);
            } catch (ProducerException e) {
                LOG.error("ProducerException while sending log to project={}, logstore={}, topic={}, buffered={}",
                        project, sinkLogStore, record.getTopic(), buffered.get(), e);
                throw new RuntimeException("Failed to send log", e);
            }
        }

        @Override
        public void close() {
        }
    }

    @Override
    public void invoke(T value, Context context) {
        if (this.producer == null) {
            LOG.error("Producer is null when invoking, project={}, logstore={}", project, logstore);
            throw new IllegalStateException("Flink log producer has not been initialized yet!");
        }
        try {
            schema.serialize(value, sinkCollector);
        } catch (Exception e) {
            LOG.error("Error serializing record for project={}, logstore={}", project, logstore, e);
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing FlinkLogProducerV2 for project={}, logstore={}, finalBuffered={}",
                project, logstore, buffered.get());
        if (producer != null) {
            try {
                producer.close();
                LOG.debug("Producer closed successfully for project={}, logstore={}", project, logstore);
            } catch (Exception e) {
                LOG.warn("Error closing producer for project={}, logstore={}", project, logstore, e);
            } finally {
                producer = null;
            }
        }
        super.close();
        LOG.info("FlinkLogProducerV2 closed successfully for project={}, logstore={}", project, logstore);
    }

    private static class ProducerCallback implements Callback {
        private final AtomicLong buffered;

        private ProducerCallback(AtomicLong buffered) {
            this.buffered = buffered;
        }

        @Override
        public void onCompletion(Result result) {
            if (result == null) {
                LOG.error("Unexpected null result in callback, buffered={}", buffered.get());
            } else if (!result.isSuccessful()) {
                LOG.error("Failed to send log: errorCode={}, errorMessage={}, retries={}, buffered={}",
                        result.getErrorCode(),
                        result.getErrorMessage(),
                        result.getAttemptCount(),
                        buffered.get());
            }
            buffered.decrementAndGet();
        }
    }
}