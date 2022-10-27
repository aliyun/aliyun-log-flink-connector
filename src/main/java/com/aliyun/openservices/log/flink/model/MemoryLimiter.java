package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.ConfigConstants;
import org.apache.flink.util.PropertiesUtil;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class MemoryLimiter implements Serializable {
    private static final int MIN_MEMORY_LIMIT_MB = 20;
    private final boolean isEnabled;
    private final Semaphore semaphore;

    public MemoryLimiter() {
        this.isEnabled = false;
        this.semaphore = null;
    }

    public MemoryLimiter(Properties configProps) {
        if (configProps.containsKey(ConfigConstants.SOURCE_MEMORY_LIMIT)) {
            int memoryLimit = PropertiesUtil.getInt(configProps, ConfigConstants.SOURCE_MEMORY_LIMIT, 0);
            if (memoryLimit < MIN_MEMORY_LIMIT_MB * 1024 * 1024) {
                throw new IllegalArgumentException("The " + ConfigConstants.SOURCE_MEMORY_LIMIT + " must not less than "
                        + MIN_MEMORY_LIMIT_MB + "MB");
            }
            this.semaphore = new Semaphore(memoryLimit);
            this.isEnabled = true;
        } else {
            this.isEnabled = false;
            this.semaphore = null;
        }
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    public boolean tryAcquire(int permits) {
        if (semaphore != null) {
            return semaphore.tryAcquire(permits);
        }
        return true;
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
