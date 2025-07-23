package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.ConfigConstants;
import org.apache.flink.util.PropertiesUtil;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class MemoryLimiter implements Serializable {
    private static final int MIN_MEMORY_LIMIT_MB = 100;
    private final Semaphore semaphore;


    public MemoryLimiter(Properties configProps) {
        if (configProps.containsKey(ConfigConstants.SOURCE_MEMORY_LIMIT)) {
            int memoryLimit = PropertiesUtil.getInt(configProps, ConfigConstants.SOURCE_MEMORY_LIMIT, 0);
            if (memoryLimit < MIN_MEMORY_LIMIT_MB * 1024 * 1024) {
                throw new IllegalArgumentException("The " + ConfigConstants.SOURCE_MEMORY_LIMIT + " must not less than "
                        + MIN_MEMORY_LIMIT_MB + "MB");
            }
            this.semaphore = new Semaphore(memoryLimit);
        } else {
            this.semaphore = null;
        }
    }

    public void acquire(int permits) throws InterruptedException {
        if (semaphore != null) {
            semaphore.acquire(permits);
        }
    }

    public void release(int permits) {
        if (semaphore != null) {
            semaphore.release(permits);
        }
    }
}
