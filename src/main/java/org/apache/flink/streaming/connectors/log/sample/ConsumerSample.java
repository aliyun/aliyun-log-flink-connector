package org.apache.flink.streaming.connectors.log.sample;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.log.ConfigConstants;
import org.apache.flink.streaming.connectors.log.FlinkLogConsumer;
import org.apache.flink.streaming.connectors.log.data.RawLogGroupList;
import org.apache.flink.streaming.connectors.log.data.RawLogGroupListDeserializer;
import org.apache.flink.streaming.connectors.log.util.Consts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConsumerSample {
    public static String sEndpoint = "cn-hangzhou.log.aliyuncs.com";
    public static String sAccessKeyId = "";
    public static String sAccessKey = "";
    public static String sProject = "ali-cn-hangzhou-sls-admin";
    public static String sLogstore = "sls_consumergroup_log";
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerSample.class);


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(3);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend("file:///Users/zhouzhou/Binary/flink-1.3.2/checkpoints/"));

        RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
        Properties configProps = new Properties();
        configProps.put(ConfigConstants.LOG_ENDPOINT, sEndpoint);
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, sAccessKeyId);
        configProps.put(ConfigConstants.LOG_ACCESSKEY, sAccessKey);
        configProps.put(ConfigConstants.LOG_PROJECT, sProject);
        configProps.put(ConfigConstants.LOG_LOGSTORE, sLogstore);
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_END_CURSOR);
        configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "consumergroup-flink");
        DataStream<RawLogGroupList> logTestStream = env.addSource(
                new FlinkLogConsumer<RawLogGroupList>(deserializer, configProps)
        );
        logTestStream.writeAsText("/Users/zhouzhou/Binary/flink-1.3.2/data/newb.txt." + System.currentTimeMillis());
        env.execute("flink log connector");
    }
}
