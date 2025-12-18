package com.aliyun.openservices.log.flink.sample;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.FastLogGroupDeserializer;
import com.aliyun.openservices.log.flink.data.FastLogGroupList;
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
        configProps.put(ConfigConstants.LOG_ACCESSKEYID, ACCESS_KEY_ID);
        configProps.put(ConfigConstants.LOG_ACCESSKEY, ACCESS_KEY_SECRET);
        configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "10");
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_FROM_CHECKPOINT);
        configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "23_ots_sla_etl_product1");
        configProps.put(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.ON_CHECKPOINTS.name());
        configProps.put(ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS, "10000");
        configProps.put(ConfigConstants.STOP_TIME, "1627878020");
        configProps.put(ConfigConstants.SIGNATURE_VERSION, "v4");
        configProps.put(ConfigConstants.REGION_ID, "cn-hangzhou");
        // SPL Consume Processor, reference: https://help.aliyun.com/zh/sls/data-consumption-processor/
        // configProps.put(ConfigConstants.PROCESSOR, "consume-processor-12345678");

        FastLogGroupDeserializer deserializer = new FastLogGroupDeserializer();
        DataStream<FastLogGroupList> stream = env.addSource(
                new FlinkLogConsumer<>(SLS_PROJECT, SLS_LOGSTORE, deserializer, configProps));

        stream.flatMap((FlatMapFunction<FastLogGroupList, String>) (value, out) -> {
            for (FastLogGroup logGroup : value.getLogGroups()) {
                int logCount = logGroup.getLogsCount();
                for (int i = 0; i < logCount; i++) {
                    FastLog log = logGroup.getLogs(i);
                    JSONObject jsonObject = new JSONObject();
                    for (int j = 0; j < log.getContentsCount(); j++) {
                        jsonObject.put(log.getContents(j).getKey(), log.getContents(j).getValue());
                    }
                    out.collect(jsonObject.toJSONString());
                }
            }
        }).returns(String.class);

        stream.writeAsText("log-" + System.nanoTime());
        env.execute("Flink consumer");
    }
}
