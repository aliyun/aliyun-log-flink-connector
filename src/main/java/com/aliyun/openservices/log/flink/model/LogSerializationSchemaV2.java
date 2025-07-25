package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.data.SinkRecord;

import java.io.Serializable;

public interface LogSerializationSchemaV2<T> extends Serializable {

    SinkRecord serialize(T element);
}
