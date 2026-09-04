package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.aliyun.log.producer.Callback;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.StaticCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.data.SinkRecord;
import com.aliyun.openservices.log.flink.model.LogSerializationSchemaV2;
import com.aliyun.openservices.log.flink.util.ConfigProperties;
import com.aliyun.openservices.log.flink.util.ConfigParser;
import com.aliyun.openservices.log.flink.util.ProducerFactory;
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
    private static final long serialVersionUID = -114178204262097392L;
    private final LogSerializationSchemaV2<T> schema;
    private final AtomicLong buffered = new AtomicLong(0);
    private transient Producer producer;
    private transient ProducerCallback callback;
    private final String project;
    private final String logstore;
    private ConfigParser configParser;
    private SinkCollector<T> sinkCollector;
    private LogCredentialsProviderFactory credentialsProviderFactory;

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

    /**
     * Sets a serializable factory that creates the SLS credentials provider at runtime.
     *
     * @param credentialsProviderFactory runtime credentials provider factory
     * @return this producer
     */
    public FlinkLogProducerV2<T> setCredentialsProviderFactory(
            LogCredentialsProviderFactory credentialsProviderFactory) {
        if (credentialsProviderFactory == null) {
            throw new IllegalArgumentException("CredentialsProviderFactory must not be null");
        }
        if (producer != null) {
            throw new IllegalStateException(
                    "CredentialsProviderFactory cannot be changed after the producer is created");
        }
        this.credentialsProviderFactory = credentialsProviderFactory;
        this.configParser = new ConfigParser(
                ConfigProperties.sanitizedCopyWithoutCredentials(
                        this.configParser.copyProperties()));
        return this;
    }

    private LogCredentialsProviderFactory getCredentialsProviderFactory(ConfigParser parser) {
        if (credentialsProviderFactory != null) {
            return credentialsProviderFactory;
        }
        return new StaticCredentialsProviderFactory(
                parser.getString(ConfigConstants.LOG_ACCESSKEYID),
                parser.getString(ConfigConstants.LOG_ACCESSKEY));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("Opening FlinkLogProducerV2 for project={}, logstore={}", project, logstore);
        if (callback == null) {
            callback = new ProducerCallback(buffered);
        }
        if (producer == null) {
            producer = ProducerFactory.create(
                    project,
                    configParser.getString(ConfigConstants.LOG_ENDPOINT),
                    configParser.copyProperties(),
                    getCredentialsProviderFactory(configParser));
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
