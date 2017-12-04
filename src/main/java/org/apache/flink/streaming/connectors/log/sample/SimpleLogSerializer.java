package org.apache.flink.streaming.connectors.log.sample;

import org.apache.flink.streaming.connectors.log.data.RawLog;
import org.apache.flink.streaming.connectors.log.data.RawLogGroup;
import org.apache.flink.streaming.connectors.log.model.LogSerializationSchema;

public class SimpleLogSerializer implements LogSerializationSchema<String> {

    public RawLogGroup serialize(String element) {
        RawLogGroup rlg = new RawLogGroup();
        RawLog rl = new RawLog();
        rl.setTime((int)(System.currentTimeMillis() / 1000));
        rl.addContent("message", element);
        rlg.addLog(rl);
        return rlg;
    }
}
