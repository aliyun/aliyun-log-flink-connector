package com.aliyun.openservices.log.flink.model;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

public interface LogDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Deserialize pull logs result to Flink records.
     *
     * @param record LogGroup list.
     * @return
     */
    T deserialize(PullLogsResult record);
}
