package com.aliyun.openservices.log.flink;

import java.io.Serializable;

public abstract class LogPartitioner<T> implements Serializable {
    public abstract String getHashKey(T element);
    public void initialize(int indexOfThisSubtask, int numberOfParallelSubtasks){

    }
}
