package com.aliyun.openservices.log.flink.sample;

import com.aliyun.openservices.log.common.*;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.flink.source.AliyunLogSource;
import com.aliyun.openservices.log.flink.source.StartingPosition;
import com.aliyun.openservices.log.flink.source.deserialization.AliyunLogDeserializationSchema;
import com.aliyun.openservices.log.flink.source.enumerator.assigner.RoundRobinSplitAssigner;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Sample demonstrating how to use the new AliyunLogSource API with SourceBuilder.
 */
public class AliyunLogSourceSample {
    // Configuration - replace with your actual values
    private static final String SLS_LOGSTORE = "your-logstore";
    private static final String CONSUMER_GROUP = "your-consumer-group";
    private static final String SLS_ENDPOINT = "your-endpoint";
    private static final String SLS_PROJECT = "your-project";
    public static final String ACCESS_KEY_ID = "";
    public static final String ACCESS_KEY_SECRET = "";

    public static class SourceRecord {
        private LogItem logItem;
        private String source;
        private String topic;

        public LogItem getLogItem() {
            return logItem;
        }

        public void setLogItem(LogItem logItem) {
            this.logItem = logItem;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Setup Flink execution environment
        Configuration conf = new Configuration();
        conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/flink-checkpoints");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, conf);
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(2); // Set parallelism for multiple readers

        // Enable checkpointing
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-state"));

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.minutes(3)));

        // Build AliyunLogSource using the builder pattern
        AliyunLogSource<SourceRecord> source = AliyunLogSource.<SourceRecord>builder()
                .setProject(SLS_PROJECT)
                .setLogStore(SLS_LOGSTORE)
                .setEndpoint(SLS_ENDPOINT)
                .setCredentials(ACCESS_KEY_ID, ACCESS_KEY_SECRET) // Set access key ID and access key
                .setDeserializer(new AliyunLogDeserializationSchema<SourceRecord>() {
                    @Override
                    public TypeInformation<SourceRecord> getProducedType() {
                        return PojoTypeInfo.of(SourceRecord.class);
                    }

                    @Override
                    public void deserialize(PullLogsResult record, Collector<SourceRecord> out) {
                        List<LogGroupData> logGroupDataList = record.getLogGroupList();
                        for (LogGroupData logGroupData : logGroupDataList) {
                            FastLogGroup logGroup = logGroupData.GetFastLogGroup();
                            for (int lIdx = 0; lIdx < logGroup.getLogsCount(); ++lIdx) {
                                FastLog log = logGroup.getLogs(lIdx);
                                LogItem logItem = new LogItem(log.getTime());
                                for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                                    FastLogContent content = log.getContents(cIdx);
                                    logItem.PushBack(content.getKey(), content.getValue());
                                }
                                SourceRecord sourceRecord = new SourceRecord();
                                sourceRecord.setLogItem(logItem);
                                sourceRecord.setSource(logGroup.getSource());
                                sourceRecord.setTopic(logGroup.getTopic());
                                out.collect(sourceRecord);
                            }
                        }
                    }
                })
                // Configure starting position
                .setStartingPosition(StartingPosition.CHECKPOINT) // or EARLIEST, LATEST, TIMESTAMP
                .setFallbackPosition(StartingPosition.EARLIEST) // Fallback if no checkpoint found
                // Configure consumer group (required for checkpoint mode)
                .setConsumerGroup(CONSUMER_GROUP)
                // Use round-robin shard assigner for balanced distribution
                .setSplitAssigner(new RoundRobinSplitAssigner())
                // Optional: Set additional properties
                .setProperty(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "100")
                .setProperty(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.ON_CHECKPOINTS.name())
                .setProperty(ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS, "10000")
                // Optional: Set signature version and region for v4 signature
                .setProperty(ConfigConstants.SIGNATURE_VERSION, "v4")
                .setProperty(ConfigConstants.REGION_ID, "cn-hangzhou")
                .build();

        // Create data stream from source
        DataStream<SourceRecord> stream = env.fromSource(
                        source,
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "AliyunLogSource")
                .disableChaining();
        stream.print();

        // Execute the job
        System.out.println("Starting Flink job: Aliyun Log Source Sample");
        env.execute("Aliyun Log Source Sample");
    }
}
