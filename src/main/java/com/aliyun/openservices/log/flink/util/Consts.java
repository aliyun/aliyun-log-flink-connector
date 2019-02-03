package com.aliyun.openservices.log.flink.util;

public class Consts {
    public static String READONLY_SHARD_STATUS = "readonly";
    public static String READWRITE_SHARD_STATUS = "readwrite";

    public static int DEFAULT_NUMBER_PER_FETCH = 100;
    public static long DEFAULT_FETCH_INTERVAL_MILLIS = 100;
    public static long DEFAULT_SHARDS_DISCOVERY_INTERVAL_MILLIS = 30 * 1000;

    public static String LOG_BEGIN_CURSOR = "begin_cursor";
    public static String LOG_END_CURSOR = "end_cursor";
    public static String LOG_FROM_CHECKPOINT = "consumer_from_checkpoint";

    public static String LOG_PRODUCER_USER_AGENT = "flink-log-producer-0.1.1";

}
