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

    static <T> T call(Callable<T> callable, String action) throws LogException {
        long backoff = INITIAL_BACKOFF;
        int retries = 0;
        do {
            try {
                return callable.call();
            } catch (LogException ex) {
                if (ex.GetHttpCode() == 400) {
                    throw ex;
                }
                // for read operation, no other way to handle 403
                if (ex.GetHttpCode() >= 500 || ex.GetHttpCode() == 403 || ex.GetHttpCode() < 0) {
                    // 500 - internal server error
                    // 403 - Quota exceed
                    // <0 - network error
                    LOG.error("{} fail: {}, retry after {} ms", action, ex.GetErrorMessage(), backoff);
                    retries = 0;
                } else if (retries < MAX_ATTEMPTS) {
                    LOG.error("{} fail: {}, retry {}/{}", action, ex.GetErrorMessage(), retries, MAX_ATTEMPTS);
                    retries++;
                } else {
                    throw ex;
                }
            } catch (Exception ex) {
                if (retries >= MAX_ATTEMPTS) {
                    throw new LogException("UnknownError", ex.getMessage(), ex, "");
                }
                LOG.error("{} fail: {}, retry {}/{}", action, ex.getMessage(), retries, MAX_ATTEMPTS, ex);
                retries++;
            }
            waitForMs(backoff);
            backoff = Math.min(backoff * 2, MAX_BACKOFF);
        } while (true);
    }
}
