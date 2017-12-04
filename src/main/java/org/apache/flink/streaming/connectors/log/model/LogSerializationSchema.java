package org.apache.flink.streaming.connectors.log.model;

import org.apache.flink.streaming.connectors.log.data.RawLogGroup;

import java.io.Serializable;

public interface LogSerializationSchema<T> extends Serializable{
    RawLogGroup serialize(T element);
}
