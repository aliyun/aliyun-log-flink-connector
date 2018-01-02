package com.aliyun.openservices.log.flink.sample;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.util.Consts;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConsumerSample {
    public static String sEndpoint = "cn-huhehaote.log.aliyuncs.com";
    public static String sAccessKeyId = "";
    public static String sAccessKey = "";
    public static String sProject = "ali-ots-sla-cn-huhehaote";
    public static String sLogstore = "ots_sla_product";
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerSample.class);


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(2);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend("file:///Users/zhouzhou/Binary/flink-1.3.2/testcheckpoints/"));
        RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
        Properties configProps = new Properties();
        configProps.put(ConfigConstants.LOG_ENDPOINT, sEndpoint);
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, sAccessKeyId);
        configProps.put(ConfigConstants.LOG_ACCESSKEY, sAccessKey);
        configProps.put(ConfigConstants.LOG_PROJECT, sProject);
        configProps.put(ConfigConstants.LOG_LOGSTORE, sLogstore);
        configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "10");
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_FROM_CHECKPOINT);
        configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "23_ots_sla_etl_product");
        DataStream<RawLogGroupList> logTestStream = env.addSource(
                new FlinkLogConsumer<RawLogGroupList>(deserializer, configProps)
        );

        logTestStream.writeAsText("/Users/zhouzhou/Binary/flink-1.3.2/data/newb.txt." + System.nanoTime());
        env.execute("flink log connector");
    }
}
