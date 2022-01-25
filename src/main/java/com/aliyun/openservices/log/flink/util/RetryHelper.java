package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.exception.LogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

final class RetryHelper {
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);

    private static final long INITIAL_BACKOFF = 200;
    private static final long MAX_BACKOFF = 5000;
    private static final int MAX_ATTEMPTS = 5;
    private static final int MAX_RETRIABLE_ERROR_ATTEMPTS = 20;
    private volatile boolean isCanceled = false;

    public void cancel() {
        isCanceled = true;
    }

    <T> T call(Callable<T> callable, String action) throws LogException {
        long backoff = INITIAL_BACKOFF;
        int retries = 0;
        LogException lastException;
        int retriableError = 0;
        do {
            try {
                return callable.call();
            } catch (LogException ex) {
                int status = ex.GetHttpCode();
                if (status == 400 || isCanceled) {
                    throw ex;
                }
                // for read operation, no other way to handle 403
                if (status >= 500 || status == 403 || status < 0) {
                    // 500 - internal server error
                    // 403 - Quota exceed
                    // < 0 - Network error
                    if (++retriableError >= MAX_RETRIABLE_ERROR_ATTEMPTS) {
                        throw ex;
                    }
                    LOG.error("{} fail: {}, sleep {}ms", action, ex.GetErrorMessage(), backoff);
                } else if (retries < MAX_ATTEMPTS) {
                    LOG.error("{} fail: {}, retry {}/{}", action, ex.GetErrorMessage(), retries, MAX_ATTEMPTS);
                    retries++;
                } else {
                    throw ex;
                }
                lastException = ex;
            } catch (Exception ex) {
                lastException = new LogException("ClientError", ex.getMessage(), ex, "");
                if (retries >= MAX_ATTEMPTS || isCanceled) {
                    throw lastException;
                }
                LOG.error("{} fail: {}, retry {}/{}", action, ex.getMessage(), retries, MAX_ATTEMPTS, ex);
                retries++;
            }
            try {
                Thread.sleep(backoff);
                if (!isCanceled) {
                    backoff = Math.min(backoff * 2, MAX_BACKOFF);
                    continue;
                }
            } catch (InterruptedException e) {
                LOG.warn("Sleep {} interrupted", backoff);
                Thread.currentThread().interrupt();
            }
            throw lastException;
        } while (true);
    }
}
