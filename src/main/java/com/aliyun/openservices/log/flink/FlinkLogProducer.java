package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.aliyun.log.producer.Callback;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.StaticCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.model.LogSerializationSchema;
import com.aliyun.openservices.log.flink.util.ConfigProperties;
import com.aliyun.openservices.log.flink.util.ConfigParser;
import com.aliyun.openservices.log.flink.util.ProducerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static com.aliyun.openservices.log.flink.ConfigConstants.*;

public class FlinkLogProducer<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkLogProducer.class);
    private static final long serialVersionUID = -906744714968340499L;
    private final LogSerializationSchema<T> schema;
    private LogPartitioner<T> customPartitioner = null;
    private transient Producer producer;
    private transient ProducerCallback callback;
    private final String project;
    private final String logstore;
    private final AtomicLong buffered = new AtomicLong(0);
    private ConfigParser configParser;
    private LogCredentialsProviderFactory credentialsProviderFactory;

    public FlinkLogProducer(final LogSerializationSchema<T> schema, Properties configProps) {
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

    public void setCustomPartitioner(LogPartitioner<T> customPartitioner) {
        this.customPartitioner = customPartitioner;
    }

    /**
     * Sets a serializable factory that creates the SLS credentials provider at runtime.
     *
     * @param credentialsProviderFactory runtime credentials provider factory
     * @return this producer
     */
    public FlinkLogProducer<T> setCredentialsProviderFactory(
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
        if (customPartitioner != null) {
            customPartitioner.initialize(
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getNumberOfParallelSubtasks());
        }
        if (callback == null) {
            callback = new ProducerCallback(buffered);
        }
        if (producer == null) {
            producer = ProducerFactory.create(
                    project,
                    configParser.getString(ConfigConstants.LOG_ENDPOINT),
                    configParser.copyProperties(),
                    getCredentialsProviderFactory(configParser));
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (producer == null) {
            return;
        }
        long beginAt = System.currentTimeMillis();
        long sleepTime = 10;
        long maxSleepTime = 100;
        while (true) {
            if (buffered.get() <= 0) {
                LOG.info("snapshotState succeed, usedTime={}", System.currentTimeMillis() - beginAt);
                break;
            }
            LOG.info("Sleep {} ms to wait all records flushed, buffered={}", sleepTime, buffered.get());
            Thread.sleep(sleepTime);
            sleepTime = Math.min(sleepTime * 2, maxSleepTime);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    }

    @Override
    public void invoke(T value, Context context) {
        if (this.producer == null) {
            throw new IllegalStateException("Flink log producer has not been initialized yet!");
        }
        RawLogGroup logGroup = schema.serialize(value);
        if (logGroup == null) {
            LOG.info("Skipping null LogGroup.");
            return;
        }
        String shardHashKey = null;
        if (customPartitioner != null) {
            shardHashKey = customPartitioner.getHashKey(value);
        }
        List<LogItem> logs = new ArrayList<>();
        for (RawLog rawLog : logGroup.getLogs()) {
            if (rawLog == null) {
                continue;
            }
            LogItem record = new LogItem(rawLog.getTime());
            for (Map.Entry<String, String> kv : rawLog.getContents().entrySet()) {
                if (kv.getKey() != null) {
                    record.PushBack(kv.getKey(), kv.getValue());
                }
            }
            logs.add(record);
        }
        if (logs.isEmpty()) {
            return;
        }
        String sinkLogStore = logGroup.getLogstore() != null ? logGroup.getLogstore() : logstore;
        try {
            producer.send(project,
                    sinkLogStore,
                    logGroup.getTopic(),
                    logGroup.getSource(),
                    shardHashKey,
                    logs,
                    callback);
            buffered.incrementAndGet();
        } catch (InterruptedException | ProducerException e) {
            LOG.error("Error while sending logs", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
            producer = null;
        }
        super.close();
        LOG.info("Flink log producer has been closed");
    }

    private static class ProducerCallback implements Callback {
        private final AtomicLong buffered;

        private ProducerCallback(AtomicLong buffered) {
            this.buffered = buffered;
        }

        @Override
        public void onCompletion(Result result) {
            if (result == null) {
                LOG.error("Unexpected null result, buffered={}", buffered.get());
            } else if (!result.isSuccessful()) {
                LOG.error("Failed to send log due to code={}, message={}, retries={}",
                        result.getErrorCode(),
                        result.getErrorMessage(),
                        result.getAttemptCount());
            }
            buffered.decrementAndGet();
        }
    }
}
