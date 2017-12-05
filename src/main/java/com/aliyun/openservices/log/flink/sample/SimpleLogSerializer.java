package com.aliyun.openservices.log.flink.sample;

import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.model.LogSerializationSchema;
import com.aliyun.openservices.log.flink.data.RawLog;

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
