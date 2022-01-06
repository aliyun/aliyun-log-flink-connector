package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.internal.ConfigWrapper;
import com.aliyun.openservices.log.flink.internal.Producer;
import com.aliyun.openservices.log.flink.internal.ProducerConfig;
import com.aliyun.openservices.log.flink.internal.ProducerImpl;
import com.aliyun.openservices.log.flink.model.LogSerializationSchema;
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

import static com.aliyun.openservices.log.flink.ConfigConstants.FLUSH_INTERVAL_MS;
import static com.aliyun.openservices.log.flink.ConfigConstants.IO_THREAD_NUM;

public class FlinkLogProducer<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkLogProducer.class);
    private final LogSerializationSchema<T> schema;
    private LogPartitioner<T> customPartitioner = null;
    private transient Producer producer;
    private final Properties properties;

    public FlinkLogProducer(final LogSerializationSchema<T> schema, Properties configProps) {
        if (schema == null) {
            throw new IllegalArgumentException("schema cannot be null");
        }
        if (configProps == null) {
            throw new IllegalArgumentException("configProps cannot be null");
        }
        this.schema = schema;
        this.properties = configProps;
    }

    public void setCustomPartitioner(LogPartitioner<T> customPartitioner) {
        this.customPartitioner = customPartitioner;
    }

    private Producer createProducer(ConfigWrapper configWrapper) {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setFlushInterval(configWrapper.getLong(FLUSH_INTERVAL_MS,
                ProducerConfig.DEFAULT_LINGER_MS));
        producerConfig.setIoThreadNum(
                configWrapper.getInt(IO_THREAD_NUM, ProducerConfig.DEFAULT_IO_THREAD_COUNT));
        producerConfig.setProject(configWrapper.getString(ConfigConstants.LOG_PROJECT));
        producerConfig.setLogstore(configWrapper.getString(ConfigConstants.LOG_LOGSTORE));
        producerConfig.setEndpoint(configWrapper.getString(ConfigConstants.LOG_ENDPOINT));
        producerConfig.setAccessKeyId(configWrapper.getString(ConfigConstants.LOG_ACCESSKEYID));
        producerConfig.setAccessKeySecret(configWrapper.getString(ConfigConstants.LOG_ACCESSKEY));
        producerConfig.setTotalSizeInBytes(configWrapper.getInt(ConfigConstants.TOTAL_SIZE_IN_BYTES,
                ProducerConfig.DEFAULT_TOTAL_SIZE_IN_BYTES));
        producerConfig.setLogGroupSize(configWrapper.getInt(ConfigConstants.LOG_GROUP_MAX_SIZE,
                ProducerConfig.DEFAULT_LOG_GROUP_SIZE));
        producerConfig.setLogGroupMaxLines(configWrapper.getInt(ConfigConstants.LOG_GROUP_MAX_LINES,
                ProducerConfig.DEFAULT_MAX_LOG_GROUP_LINES));
        return new ProducerImpl(producerConfig);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (customPartitioner != null) {
            customPartitioner.initialize(
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getNumberOfParallelSubtasks());
        }
        if (producer == null) {
            ConfigWrapper configWrapper = new ConfigWrapper(properties);
            producer = createProducer(configWrapper);
            producer.open();
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (producer != null) {
            producer.flush();
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
            LOG.info("Skip null LogGroup");
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
                String key = kv.getKey();
                if (key == null) {
                    continue;
                }
                record.PushBack(key, kv.getValue());
            }
            logs.add(record);
        }
        if (logs.isEmpty()) {
            return;
        }
        try {
            producer.send(logGroup.getTopic(), logGroup.getSource(), shardHashKey, logs);
        } catch (InterruptedException e) {
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
}
