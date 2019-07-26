package com.aliyun.openservices.log.flink.sample;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.util.Consts;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class ConsumerSample {
    private static final String SLS_ENDPOINT = "";
    private static final String ACCESS_KEY_ID = "";
    private static final String ACCESS_KEY_SECRET = "";
    private static final String SLS_PROJECT = "";
    private static final String SLS_LOGSTORE = "";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // For local testing
        Configuration conf = new Configuration();
        conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                "file:///Users/kel/Github/flink3/aliyun-log-flink-connector/flink2");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend("file:///Users/kel/Github/flink3/aliyun-log-flink-connector/flink"));
        Properties configProps = new Properties();
        configProps.put(ConfigConstants.LOG_ENDPOINT, SLS_ENDPOINT);
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, ACCESS_KEY_ID);
        configProps.put(ConfigConstants.LOG_ACCESSKEY, ACCESS_KEY_SECRET);
        configProps.put(ConfigConstants.LOG_PROJECT, SLS_PROJECT);
        configProps.put(ConfigConstants.LOG_LOGSTORE, SLS_LOGSTORE);
        configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "10");
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_FROM_CHECKPOINT);
        configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "23_ots_sla_etl_product1");
        configProps.put(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.ON_CHECKPOINTS.name());
        configProps.put(ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS, "10000");
        RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
        DataStream<RawLogGroupList> stream = env.addSource(
                new FlinkLogConsumer<RawLogGroupList>(deserializer, configProps));

        stream.flatMap(new FlatMapFunction<RawLogGroupList, String>() {
            @Override
            public void flatMap(RawLogGroupList value, Collector<String> out) throws Exception {
                for (RawLogGroup logGroup : value.getRawLogGroups()) {
                    for (RawLog log : logGroup.getLogs()) {
                        out.collect(log.getContents().get("content"));
                    }
                }
            }
        });

        stream.writeAsText("/Users/kel/Github/flink3/aliyun-log-flink-connector/flink/newb.txt." + System.nanoTime());
        env.execute("Flink-log-connector");
    }
}
