package com.aliyun.openservices.log.flink.util;

public class RetryPolicy {

    private int maxRetries;
    private int maxRetriesForRetryableError;
    private long baseRetryBackoff;
    private long maxRetryBackoff;

    public static RetryPolicyBuilder builder() {
        return new RetryPolicyBuilder();
    }

    public static final class RetryPolicyBuilder {
        private int maxRetries;
        private int maxRetriesForRetryableError;
        private long baseRetryBackoff;
        private long maxRetryBackoff;

        private RetryPolicyBuilder() {
        }

        public RetryPolicyBuilder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public RetryPolicyBuilder maxRetriesForRetryableError(int maxRetriesForRetryableError) {
            this.maxRetriesForRetryableError = maxRetriesForRetryableError;
            return this;
        }

        public RetryPolicyBuilder baseRetryBackoff(long baseRetryBackoff) {
            this.baseRetryBackoff = baseRetryBackoff;
            return this;
        }

        public RetryPolicyBuilder maxRetryBackoff(long maxRetryBackoff) {
            this.maxRetryBackoff = maxRetryBackoff;
            return this;
        }

        public RetryPolicy build() {
            RetryPolicy retryPolicy = new RetryPolicy();
            retryPolicy.maxRetryBackoff = this.maxRetryBackoff;
            retryPolicy.baseRetryBackoff = this.baseRetryBackoff;
            retryPolicy.maxRetries = this.maxRetries;
            retryPolicy.maxRetriesForRetryableError = this.maxRetriesForRetryableError;
            return retryPolicy;
        }
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getMaxRetriesForRetryableError() {
        return maxRetriesForRetryableError;
    }

    public long getBaseRetryBackoff() {
        return baseRetryBackoff;
    }

    public long getMaxRetryBackoff() {
        return maxRetryBackoff;
    }
}
