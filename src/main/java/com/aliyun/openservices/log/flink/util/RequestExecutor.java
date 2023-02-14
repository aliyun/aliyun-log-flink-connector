package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.exception.LogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

final class RequestExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);

    private volatile boolean isCanceled = false;
    private final int maxRetries;
    private final int maxRetriesForRetryableError;
    private final long baseBackoff;
    private final long maxBackoff;

    public RequestExecutor(RetryPolicy retryPolicy) {
        this.maxRetries = retryPolicy.getMaxRetries();
        this.maxRetriesForRetryableError = retryPolicy.getMaxRetriesForRetryableError();
        this.baseBackoff = retryPolicy.getBaseRetryBackoff();
        this.maxBackoff = retryPolicy.getMaxRetryBackoff();
    }

    public void cancel() {
        isCanceled = true;
    }

    <T> T call(Callable<T> callable, String action) throws LogException {
        return call(callable, true, action);
    }

    <T> T call(Callable<T> callable, boolean retryOn403, String action) throws LogException {
        long backoff = baseBackoff;
        int retries = 0;
        LogException lastException;
        int retryableError = 0;
        do {
            try {
                return callable.call();
            } catch (LogException ex) {
                int status = ex.GetHttpCode();
                if (status == 400 || isCanceled || (status == 403 && !retryOn403)) {
                    throw ex;
                }
                if (status >= 500 || status == 403 || status < 0) {
                    // 500 - internal server error
                    // 403 - Quota exceed, no better way to handle 403 for reading
                    // < 0 - Network error
                    if (maxRetriesForRetryableError >= 0 && ++retryableError >= maxRetriesForRetryableError) {
                        throw ex;
                    }
                    LOG.error("{} fail: {}, sleep {}ms", action, ex.GetErrorMessage(), backoff);
                } else if (retries < maxRetries) {
                    LOG.error("{} fail: {}, retry {}/{}", action, ex.GetErrorMessage(), retries, maxRetries);
                    retries++;
                } else {
                    throw ex;
                }
                lastException = ex;
            } catch (Exception ex) {
                lastException = new LogException("ClientError", ex.getMessage(), ex, "");
                if (retries >= maxRetries || isCanceled) {
                    throw lastException;
                }
                LOG.error("{} fail: {}, retry {}/{}", action, ex.getMessage(), retries, maxRetries, ex);
                retries++;
            }
            try {
                Thread.sleep(backoff);
                if (!isCanceled) {
                    backoff = Math.min(backoff * 2, maxBackoff);
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
