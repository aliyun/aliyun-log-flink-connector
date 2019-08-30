package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import org.apache.flink.util.PropertiesUtil;

import java.util.Properties;

public final class LogUtil {

    private LogUtil() {
    }

    public static CheckpointMode parseCheckpointMode(Properties properties) {
        final String mode = properties.getProperty(ConfigConstants.LOG_CHECKPOINT_MODE);
        if (mode == null || mode.isEmpty()) {
            return CheckpointMode.ON_CHECKPOINTS;
        }
        return CheckpointMode.fromString(mode);
    }

    public static long getDiscoveryIntervalMs(Properties props) {
        return PropertiesUtil.getLong(props,
                ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS,
                Consts.DEFAULT_SHARDS_DISCOVERY_INTERVAL_MILLIS);
    }

    public static long getCommitIntervalMs(Properties props) {
        return PropertiesUtil.getLong(props,
                ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS,
                Consts.DEFAULT_COMMIT_INTERVAL_MILLIS);
    }

    public static int getNumberPerFetch(Properties properties) {
        return PropertiesUtil.getInt(properties,
                ConfigConstants.LOG_MAX_NUMBER_PER_FETCH,
                Consts.DEFAULT_NUMBER_PER_FETCH);
    }

    public static long getFetchIntervalMillis(Properties properties) {
        return PropertiesUtil.getLong(properties,
                ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS,
                Consts.DEFAULT_FETCH_INTERVAL_MILLIS);
    }

    public static String getDefaultPosition(Properties properties) {
        final String val = properties.getProperty(ConfigConstants.LOG_CONSUMER_DEFAULT_POSITION);
        return val != null && !val.isEmpty() ? val : Consts.LOG_BEGIN_CURSOR;
    }
}
