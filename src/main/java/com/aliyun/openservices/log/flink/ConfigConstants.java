package com.aliyun.openservices.log.flink;

public class ConfigConstants {
    public static String LOG_ENDPOINT = "ENDPOINT";
    public static String LOG_ACCESSSKEYID = "ACCESSKEYID";
    public static String LOG_ACCESSKEY = "ACCESSKEY";
    public static String LOG_PROJECT = "PROJECT";
    public static String LOG_LOGSTORE = "LOGSTORE";
    public static String LOG_USER_AGENT = "USER_AGENT";

    //for consumer
    public static String LOG_CONSUMERGROUP = "CONSUMER_GROUP";
    public static String LOG_FETCH_DATA_INTERVAL_MILLIS = "FETCH_DATA_INTERVAL_MILLIS";
    public static String LOG_MAX_NUMBER_PER_FETCH = "MAX_NUMBER_PER_FETCH";
    public static String LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS = "SHARDS_DISCOVERY_INTERVAL";
    /**
     * Support begin_cursor, end_cursor, unix timestamp or consumer_from_checkpoint.
     */
    public static String LOG_CONSUMER_BEGIN_POSITION = "CONSUMER_BEGIN_POSITION";
    /**
     * Optional default position which will be used if the position is consumer_from_checkpoint
     * and the cursor is not valid.
     */
    public static String LOG_CONSUMER_DEFAULT_POSITION = "CONSUMER_DEFAULT_POSITION";
    public static String LOG_CHECKPOINT_MODE = "LOG_CHECKPOINT_MODE";
    public static String LOG_COMMIT_INTERVAL_MILLIS = "LOG_COMMIT_INTERVAL";


    public static final String FLUSH_INTERVAL_MS = "flush.interval.ms";
    public static final String MAX_RETRIES = "max.retries";

    /**
     * initial retry back off time.
     */
    public static final String BASE_RETRY_BACK_OFF_TIME_MS =
            "base.retry.backoff.time.ms";

    /**
     * max retry back off time.
     */
    public static final String MAX_RETRY_BACK_OFF_TIME_MS = "max.retry.backoff.time.ms";

    /**
     * wait forever while sending record.
     */
    public static final String MAX_BLOCK_TIME_MS = "max.block.time.ms";

    /**
     * io thread num of sls producer
     */
    public static final String IO_THREAD_NUM = "io.thread.num";
}
