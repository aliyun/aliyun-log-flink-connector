package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.flink.model.LogSerializationSchema;
import com.aliyun.openservices.log.producer.LogProducer;
import com.aliyun.openservices.log.producer.ProducerConfig;
import com.aliyun.openservices.log.producer.ProjectConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.util.Consts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FlinkLogProducer<T> extends RichSinkFunction<T> implements CheckpointedFunction{

    private static final Logger LOG = LoggerFactory.getLogger(FlinkLogProducer.class);
    private final Properties configProps;
    private final LogSerializationSchema<T> schema;
    private LogPartitioner<T> customPartitioner = null;
    private transient LogProducer logProducer;
    private transient ProducerCallback callback;
    private final String logProject;
    private final String logStore;

    public FlinkLogProducer(final LogSerializationSchema<T> schema, Properties configProps){
        this.configProps = configProps;
        this.schema = schema;
        logProject = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        logStore = configProps.getProperty(ConfigConstants.LOG_LOGSTORE);

    }

    public Properties getConfigProps() {
        return configProps;
    }

    public LogPartitioner<T> getCustomPartitioner() {
        return customPartitioner;
    }

    public void setCustomPartitioner(LogPartitioner<T> customPartitioner) {
        this.customPartitioner = customPartitioner;
    }

    public LogSerializationSchema<T> getSchema() {
        return schema;
    }

    public ProducerCallback getCallback() {
        return callback;
    }

    public void setCallback(ProducerCallback callback) {
        this.callback = callback;
    }

    @Override
    public void open(Configuration parameters) throws Exception
    {
        super.open(parameters);
        if(callback == null){
            callback = new ProducerCallback();
        }
        if(customPartitioner != null){
            customPartitioner.initialize(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
        }
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.userAgent = Consts.LOG_PRODUCER_USER_AGENT;
        if(configProps.containsKey(ConfigConstants.LOG_SENDER_IO_THREAD_COUNT))
            producerConfig.maxIOThreadSizeInPool = Integer.valueOf(configProps.getProperty(ConfigConstants.LOG_SENDER_IO_THREAD_COUNT));
        if (configProps.containsKey(ConfigConstants.LEGACY_LOG_PACKAGE_TIMEOUT_MILLIS)) {
            LOG.warn("Setting {} has been deprecated in favor of {}", ConfigConstants.LEGACY_LOG_PACKAGE_TIMEOUT_MILLIS,
                    ConfigConstants.LOG_PACKAGE_TIMEOUT_MILLIS);
            producerConfig.packageTimeoutInMS = Integer.valueOf(configProps.getProperty(ConfigConstants.LEGACY_LOG_PACKAGE_TIMEOUT_MILLIS));
        } else if(configProps.containsKey(ConfigConstants.LOG_PACKAGE_TIMEOUT_MILLIS))
            producerConfig.packageTimeoutInMS = Integer.valueOf(configProps.getProperty(ConfigConstants.LOG_PACKAGE_TIMEOUT_MILLIS));
        if(configProps.containsKey(ConfigConstants.LOG_LOGS_COUNT_PER_PACKAGE))
            producerConfig.logsCountPerPackage = Integer.valueOf(configProps.getProperty(ConfigConstants.LOG_LOGS_COUNT_PER_PACKAGE));
        if(configProps.containsKey(ConfigConstants.LOG_LOGS_BYTES_PER_PACKAGE))
            producerConfig.logsBytesPerPackage = Integer.valueOf(configProps.getProperty(ConfigConstants.LOG_LOGS_BYTES_PER_PACKAGE));
        if(configProps.containsKey(ConfigConstants.LOG_MEM_POOL_BYTES))
            producerConfig.memPoolSizeInByte = Integer.valueOf(configProps.getProperty(ConfigConstants.LOG_MEM_POOL_BYTES));
        producerConfig.userAgent = "flink-log-producer";
        logProducer = new LogProducer(producerConfig);
        logProducer.setProjectConfig(new ProjectConfig(logProject, configProps.getProperty(ConfigConstants.LOG_ENDPOINT), configProps.getProperty(ConfigConstants.LOG_ACCESSSKEYID), configProps.getProperty(ConfigConstants.LOG_ACCESSKEY)));
        LOG.info("Started log producer instance");

    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if(logProducer != null) {
            logProducer.flush();
            Thread.sleep(logProducer.getProducerConfig().packageTimeoutInMS);
        }
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {
        // do nothing
    }

    public void invoke(T value) throws Exception {
        if (this.logProducer == null) {
            throw new RuntimeException("log producer has been closed");
        }
        RawLogGroup logGroup = schema.serialize(value);
        String shardHashKey = null;
        if(customPartitioner != null){
            shardHashKey = customPartitioner.getHashKey(value);
        }
        List<LogItem> logs = new ArrayList<LogItem>();
        for(RawLog rlog: logGroup.getLogs()) {
            LogItem li = new LogItem(rlog.getTime());
            for(Map.Entry<String, String> kv: rlog.getContents().entrySet()) {
                li.PushBack(kv.getKey(), kv.getValue());
            }
            logs.add(li);
        }
        ProducerCallback cloneCallback = callback.clone();
        cloneCallback.init(logProducer, logProject, logStore, logGroup.getTopic(), shardHashKey, logGroup.getSource(), logs);
        logProducer.send(logProject, logStore, logGroup.getTopic(), shardHashKey, logGroup.getSource(), logs, cloneCallback);
        if(LOG.isDebugEnabled()){
            LOG.debug("send logs...");
        }
    }
    @Override
    public void close() throws Exception {
        if(logProducer != null) {
            logProducer.flush();
            Thread.sleep(logProducer.getProducerConfig().packageTimeoutInMS);
            logProducer.close();
            logProducer = null;
        }
        super.close();
        LOG.info("closed flink log producer");

    }
}
