package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.exception.LogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

final class RetryUtil {
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);

    private static final long INITIAL_BACKOFF = 200;
    private static final long MAX_BACKOFF = 5000;
    private static final int MAX_ATTEMPTS = 5;

    private RetryUtil() {
    }

    private static void waitForMs(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static <T> T call(Callable<T> callable, String errorMsg) throws LogException {
        long backoff = INITIAL_BACKOFF;
        int retries = 0;
        do {
            try {
                return callable.call();
            } catch (LogException ex) {
                if (ex.GetHttpCode() == 400) {
                    throw ex;
                }
                if (ex.GetHttpCode() >= 500) {
                    LOG.error("{}: {}, retry after {} ms", errorMsg, ex.GetErrorMessage(), backoff);
                    retries = 0;
                } else if (retries < MAX_ATTEMPTS) {
                    LOG.error("{}: {}, retry {}/{}", errorMsg, ex.GetErrorMessage(), retries, MAX_ATTEMPTS);
                    retries++;
                } else {
                    throw ex;
                }
            } catch (Exception ex) {
                if (retries >= MAX_ATTEMPTS) {
                    throw new LogException("UnknownError", errorMsg, ex, "");
                }
                LOG.error("{}, retry {}/{}", errorMsg, retries, MAX_ATTEMPTS, ex);
                retries++;
            }
            waitForMs(backoff);
            backoff = Math.min(backoff * 2, MAX_BACKOFF);
        } while (true);
    }
}
