package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.data.SinkRecord;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public interface LogSerializationSchemaV2<T> extends Serializable {

    void serialize(T element, Collector<SinkRecord> output);
}
