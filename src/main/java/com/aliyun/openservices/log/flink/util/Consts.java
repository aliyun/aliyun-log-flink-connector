package com.aliyun.openservices.log.flink.util;

public class Consts {
    public static String READONLY_SHARD_STATUS = "readonly";
    public static String READWRITE_SHARD_STATUS = "readwrite";

    public static int DEFAULT_NUMBER_PER_FETCH = 100;
    public static long DEFAULT_FETCH_INTERVAL_MILLIS = 100;
    public static long DEFAULT_SHARDS_DISCOVERY_INTERVAL_MILLIS = 60 * 1000;
    public static long DEFAULT_COMMIT_INTERVAL_MILLIS = 10 * 1000;

    public static String LOG_BEGIN_CURSOR = "begin_cursor";
    public static String LOG_END_CURSOR = "end_cursor";
    public static String LOG_FROM_CHECKPOINT = "consumer_from_checkpoint";

    /**
     * Default retry 5 times for common errors.
     */
    public static final int DEFAULT_MAX_RETRIES = 5;
    /**
     * -1 means retry until success
     */
    public static final int DEFAULT_MAX_RETRIES_FOR_RETRYABLE_ERROR = 60;

    public static final long DEFAULT_BASE_RETRY_BACKOFF_TIME_MS = 200;

    public static final long DEFAULT_MAX_RETRY_BACKOFF_TIME_MS = 5000;
    public static final String FLINK_CONNECTOR_VERSION = "0.1.32";
}
