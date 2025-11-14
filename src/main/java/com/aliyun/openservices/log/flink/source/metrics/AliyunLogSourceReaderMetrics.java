package com.aliyun.openservices.log.flink.source.metrics;

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import java.util.concurrent.atomic.AtomicBoolean;

public class AliyunLogSourceReaderMetrics {

    // For currentFetchEventTimeLag metric
    private volatile long currentFetchEventTimeLag = Long.MAX_VALUE;
    private final AtomicBoolean isInitialized;
    private final SourceReaderMetricGroup sourceReaderMetricGroup;

    public AliyunLogSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.sourceReaderMetricGroup = sourceReaderMetricGroup;
        this.isInitialized = new AtomicBoolean(false);
    }

    public void recordFetchEventTimeLag(long lag) {
        currentFetchEventTimeLag = lag;
        if (isInitialized.compareAndSet(false, true)) {
            sourceReaderMetricGroup.gauge(
                    MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, () -> currentFetchEventTimeLag);
        }
    }
}