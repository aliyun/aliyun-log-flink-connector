package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.LogGroupData;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;
import java.util.List;

public interface LogDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    T deserialize(List<LogGroupData> logGroup);
}
