package org.apache.flink.streaming.connectors.log;

import java.io.Serializable;

public abstract class LogPartitioner<T> implements Serializable {
    public abstract String getHashKey(T element);
    public void initialize(int indexOfThisSubtask, int numberOfParallelSubtasks){

    }
}
