package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.model.LogSerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FlinkLogProducer<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkLogProducer.class);
    private final LogSerializationSchema<T> schema;
    private LogPartitioner<T> customPartitioner = null;
    private transient Producer producer;
    private transient ProducerCallback callback;
    private String logProject;
    private String logstore;
    private ExecutorService executor;
    private Properties properties;
    private ProducerConfig producerConfig;
    private AtomicLong buffered = new AtomicLong(0);

    public FlinkLogProducer(final LogSerializationSchema<T> schema, Properties configProps) {
        this(schema, configProps, new ProducerConfig());
    }

    public FlinkLogProducer(final LogSerializationSchema<T> schema,
                            Properties configProps,
                            ProducerConfig producerConfig) {
        if (schema == null) {
            throw new IllegalArgumentException("schema cannot be null");
        }
        if (configProps == null) {
            throw new IllegalArgumentException("configProps cannot be null");
        }
        if (producerConfig == null) {
            throw new IllegalArgumentException("Producer config cannot be null");
        }
        this.schema = schema;
        this.properties = configProps;
        this.producerConfig = producerConfig;
    }

    public void setCustomPartitioner(LogPartitioner<T> customPartitioner) {
        this.customPartitioner = customPartitioner;
    }

    private Producer createProducer(Properties configProps, ProducerConfig producerConfig) {
        Producer producer = new LogProducer(producerConfig);
        ProjectConfig config = new ProjectConfig(logProject,
                configProps.getProperty(ConfigConstants.LOG_ENDPOINT),
                configProps.getProperty(ConfigConstants.LOG_ACCESSSKEYID),
                configProps.getProperty(ConfigConstants.LOG_ACCESSKEY));
        producer.putProjectConfig(config);
        return producer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (callback == null) {
            callback = new ProducerCallback(buffered);
        }
        if (customPartitioner != null) {
            customPartitioner.initialize(
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getNumberOfParallelSubtasks());
        }
        if (producer == null) {
            this.producer = createProducer(properties, producerConfig);
        }
        executor = Executors.newSingleThreadExecutor();
        this.logProject = properties.getProperty(ConfigConstants.LOG_PROJECT);
        this.logstore = properties.getProperty(ConfigConstants.LOG_LOGSTORE);
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (producer != null) {
            // TODO support flush in producer
            long lingerMs = producer.getProducerConfig().getLingerMs();
            while (buffered.get() > 0) {
                LOG.info("Sleep {} ms to wait all records flushed", lingerMs);
                Thread.sleep(lingerMs);
            }
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
            LOG.info("The serialized log group is null, will not send any data to log service");
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
                record.PushBack(kv.getKey(), kv.getValue());
            }
            logs.add(record);
        }
        if (logs.isEmpty()) {
            return;
        }
        try {
            ListenableFuture<Result> future = producer.send(logProject,
                    logstore,
                    logGroup.getTopic(),
                    logGroup.getSource(),
                    shardHashKey,
                    logs);
            Futures.addCallback(future, callback, executor);
            buffered.incrementAndGet();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ProducerException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
            producer = null;
        }
        if (executor != null) {
            executor.shutdown();
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    LOG.warn("Stop executor failed");
                }
            }
        }
        super.close();
        LOG.info("Flink log producer has been closed");
    }

    public static class ProducerCallback implements FutureCallback<Result> {
        private AtomicLong count;

        ProducerCallback(AtomicLong count) {
            this.count = count;
        }

        @Override
        public void onSuccess(@Nullable Result result) {
            count.decrementAndGet();
            if (result != null && !result.isSuccessful()) {
                LOG.error("Send logs failed, code={}, errorMsg={}, retries={}",
                        result.getErrorCode(),
                        result.getErrorMessage(),
                        result.getAttemptCount());
            }
        }

        @Override
        public void onFailure(Throwable throwable) {
            count.decrementAndGet();
            LOG.error("Send logs failed", throwable);
        }
    }
}
