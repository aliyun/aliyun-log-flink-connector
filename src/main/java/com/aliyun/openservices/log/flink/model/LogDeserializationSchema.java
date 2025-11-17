package com.aliyun.openservices.log.flink.model;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

public interface LogDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Deserialize pull logs result to Flink records.
     *
     * @param record the PullLogsResult containing log groups to deserialize
     * @return the deserialized record
     */
    T deserialize(PullLogsResult record);
}
