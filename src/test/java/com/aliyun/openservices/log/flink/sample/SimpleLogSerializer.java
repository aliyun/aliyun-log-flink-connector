package com.aliyun.openservices.log.flink.sample;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.model.LogSerializationSchema;

public class SimpleLogSerializer implements LogSerializationSchema<String> {

    public static final String TAG_PREFIX = "__tag__:";

    public RawLogGroup serialize(String element) {
        RawLogGroup logGroup = new RawLogGroup();
        RawLog rawLog = new RawLog();
        rawLog.setTime((int) (System.currentTimeMillis() / 1000));
        JSONObject jsonObject = JSONObject.parseObject(element);
        for (String key : jsonObject.keySet()) {
            if (key.startsWith(TAG_PREFIX)) {
                String tag = key.substring(TAG_PREFIX.length());
                logGroup.addTag(tag, jsonObject.getString(key));
            } else {
                rawLog.addContent(key, jsonObject.getString(key));
            }
        }
        logGroup.addLog(rawLog);
        return logGroup;
    }
}
