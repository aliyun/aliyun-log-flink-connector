package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.CheckpointMode;

import java.util.Properties;

public final class LogUtil {

    private LogUtil() {
    }

    public static CheckpointMode parseCheckpointMode(Properties properties) {
        final String mode = properties.getProperty(ConfigConstants.LOG_CHECKPOINT_MODE);
        if (mode == null || mode.isEmpty()) {
            // For BWC
            return CheckpointMode.ON_CHECKPOINT;
        }
        return CheckpointMode.fromString(mode);
    }
}
