package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.exception.LogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

class RetryUtil {
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);

    private static final long INITIAL_BACKOFF = 500;
    private static final long MAX_BACKOFF = 5000;
    private static final int MAX_ATTEMPTS = 32;

    private static void waitForMs(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean isRecoverableException(LogException lex) {
        return lex != null
                && (lex.GetHttpCode() >= 500 // Internal Server Error
                || lex.GetHttpCode() == 403 // Throttling Error
                || lex.GetHttpCode() == -1); // Client error like connection timeout
    }

    private static boolean shouldStop(LogException ex, int retry) {
        if (isRecoverableException(ex)) {
            return false;
        }
        return retry > MAX_ATTEMPTS;
    }

    static <T> T retryCall(Callable<T> callable, String errorMsg) throws LogException {
        int counter = 0;
        long backoff = INITIAL_BACKOFF;
        while (counter <= MAX_ATTEMPTS) {
            try {
                return callable.call();
            } catch (LogException e1) {
                if (shouldStop(e1, counter)) {
                    // TODO Throw RuntimeException here
                    throw e1;
                }
                LOG.error("{}: {}, retry {}/{}", counter, MAX_ATTEMPTS, errorMsg, e1.GetErrorMessage());
            } catch (Exception e2) {
                if (counter >= MAX_ATTEMPTS) {
                    throw new RuntimeException(errorMsg, e2);
                }
                LOG.error("{}, retry {}/{}", counter, MAX_ATTEMPTS, errorMsg, e2);
            }
            if (counter < MAX_ATTEMPTS) {
                waitForMs(backoff);
                backoff = Math.min(backoff * 2, MAX_BACKOFF);
                counter++;
            }
        }
        throw new RuntimeException("Not possible!");
    }
}
