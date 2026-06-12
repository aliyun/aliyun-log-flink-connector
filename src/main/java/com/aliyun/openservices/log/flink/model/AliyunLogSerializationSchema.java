package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.data.SinkRecord;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Serializes user records into one or more Aliyun Log sink records.
 */
public interface AliyunLogSerializationSchema<T> extends Serializable {

    void serialize(T element, Collector<SinkRecord> output);
}
