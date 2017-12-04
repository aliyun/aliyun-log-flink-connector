package org.apache.flink.streaming.connectors.log;

public class ConfigConstants {
    //common
    public static String LOG_ENDPOINT = "ENDPOINT";
    public static String LOG_ACCESSSKEYID = "ACCESSKEYID";
    public static String LOG_ACCESSKEY = "ACCESSKEY";
    public static String LOG_PROJECT = "PROJECT";
    public static String LOG_LOGSTORE = "LOGSTORE";

    //for consumer
    public static String LOG_CONSUMERGROUP = "CONSUMER_GROUP";
    public static String LOG_FETCH_DATA_INTERVAL_MILLIS = "FETCH_DATA_INTERVAL_MILLIS";
    public static String LOG_MAX_NUMBER_PER_FETCH = "MAX_NUMBER_PER_FETCH";
    public static String LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS = "SHARDS_DISCOVERY_INTERVAL";
    public static String LOG_CONSUMER_BEGIN_POSITION = "CONSUMER_BEGIN_POSITION";

    // for producer
    public static String LOG_SENDER_IO_THREAD_COUNT = "SENDER_IO_THREAD_COUNT";
    public static String LOG_PACKAGE_TIMEOUT_MILLIS = "PACKAGE_TIMEOUUT_MILLIS";
    public static String LOG_LOGS_COUNT_PER_PACKAGE = "LOGS_COUNT_PER_PACKAGE";
    public static String LOG_LOGS_BYTES_PER_PACKAGE = "LOGS_BYTES_PER_PACKAGE";
    public static String LOG_MEM_POOL_BYTES = "MEM_POOL_SIZE_IN_BYTES";

}
