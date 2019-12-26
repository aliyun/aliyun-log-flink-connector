package com.aliyun.openservices.log.flink.sample;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.util.Consts;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


public class HDFSSinkSample {

    private static final String ACCESS_KEY_ID = "";
    private static final String ACCESS_KEY_SECRET = "";
    private static final String SLS_ENDPOINT = "";
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
        configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "1000");
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_BEGIN_CURSOR);
        configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "test-my-consumer-not-exist-2");
        configProps.put(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.ON_CHECKPOINTS.name());
        configProps.put(ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS, "10000");
        RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
        DataStream<RawLogGroupList> stream = env.addSource(
                new FlinkLogConsumer<RawLogGroupList>(deserializer, configProps));

        DataStream<RawLog> logStreams = stream.flatMap(new FlatMapFunction<RawLogGroupList, RawLog>() {
            @Override
            public void flatMap(RawLogGroupList value, Collector<RawLog> out) throws Exception {
                if (value == null) {
                    return;
                }
                for (RawLogGroup logGroup : value.getRawLogGroups()) {
                    if (logGroup == null) {
                        continue;
                    }
                    for (RawLog log : logGroup.getLogs()) {
                        out.collect(log);
                    }
                }
            }
        });
        StreamingFileSink<RawLog> sink1 = StreamingFileSink.forRowFormat(
                new Path("/tmp/hdfs/path"), // Set your HDFS path
                new Encoder<RawLog>() {

                    @Override
                    public void encode(RawLog element, OutputStream stream) throws IOException {
                        // convert to JSON or other type
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("__time__", element.getTime());
                        jsonObject.putAll(element.getContents());
                        String asJSON = jsonObject.toJSONString();
                        stream.write(asJSON.getBytes(StandardCharsets.UTF_8));
                        stream.write('\n');
                    }
                }
        ).withBucketAssigner(new EventTimeBucketAssigner())
                .build();

        logStreams.addSink(sink1);
        env.execute("HDFS sink");
    }

    private static class EventTimeBucketAssigner implements BucketAssigner<RawLog, String> {

        @Override
        public String getBucketId(RawLog element, Context context) {
            long date = (long) element.getTime() * 1000;
            String partitionValue = new SimpleDateFormat("yyyyMMdd").format(new Date(date));
            return "dt=" + partitionValue;
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
