package com.aliyun.openservices.log.flink.source.reader;

import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.flink.source.deserialization.AliyunLogDeserializationSchema;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RecordEmitter for AliyunLogSource that uses AliyunLogDeserializationSchema.
 */
public class AliyunLogRecordEmitter<T> implements RecordEmitter<PullLogsResult, T, AliyunLogSourceSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(AliyunLogRecordEmitter.class);
    private final AliyunLogDeserializationSchema<T> deserializer;
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();

    public AliyunLogRecordEmitter(AliyunLogDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void emitRecord(
            PullLogsResult element,
            SourceOutput<T> output,
            AliyunLogSourceSplitState splitState) throws Exception {
        sourceOutputWrapper.setSourceOutput(output);
        sourceOutputWrapper.setTimestamp(element.getCursorTime());
        // Use AliyunLogDeserializationSchema which supports Collector pattern
        deserializer.deserialize(element, sourceOutputWrapper);
        splitState.setNextCursor(element.getNextCursor());
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {

        private SourceOutput<T> sourceOutput;
        private long timestamp;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record, timestamp);
        }

        @Override
        public void close() {
        }

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}

