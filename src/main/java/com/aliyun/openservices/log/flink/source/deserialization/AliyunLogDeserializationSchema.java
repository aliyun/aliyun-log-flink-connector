package com.aliyun.openservices.log.flink.source.deserialization;

import com.aliyun.openservices.log.flink.model.PullLogsResult;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public interface AliyunLogDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Deserialize pull logs result to Flink records.
     *
     * @param record LogGroup list.
     * @return
     */
    void deserialize(PullLogsResult record, Collector<T> out);
}
