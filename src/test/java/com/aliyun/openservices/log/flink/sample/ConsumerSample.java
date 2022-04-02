package com.aliyun.openservices.log.flink.sample;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.model.SourceRecord;
import com.aliyun.openservices.log.flink.util.Consts;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

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
        configProps.put(ConfigConstants.SHARD_IDLE_INTERVAL_MILLIS, "60000");
        configProps.put(ConfigConstants.STOP_TIME, "1627878020");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkLogConsumer consumer = new FlinkLogConsumer(SLS_PROJECT, SLS_LOGSTORE, configProps);
        consumer.setPeriodicWatermarkAssigner(new TimeLagWatermarkGenerator());
        DataStream<SourceRecord> stream = env.addSource(consumer);
        stream.addSink(new SinkFunction<SourceRecord>() {
            @Override
            public void invoke(SourceRecord value, Context context) throws Exception {
                System.out.println("Got " + value.getRecords().size() + " records");
            }
        });
        stream.writeAsText("log-" + System.nanoTime());
        env.execute("Flink consumer");


    }

    /**
     * This generator generates watermarks that are lagging behind processing time by a fixed amount.
     * It assumes that elements arrive in Flink after a bounded delay.
     */
    public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<SourceRecord> {

        private final long maxTimeLag = 5000; // 5 seconds

        @Override
        public long extractTimestamp(SourceRecord element, long previousElementTimestamp) {
            return element.getTimestamp();
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }
}
