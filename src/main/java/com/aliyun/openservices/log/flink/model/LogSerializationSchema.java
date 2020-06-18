package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.data.RawLogGroup;

import java.io.Serializable;

public interface LogSerializationSchema<T> extends Serializable {

    RawLogGroup serialize(T element);
}
