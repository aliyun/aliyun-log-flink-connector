package com.aliyun.openservices.log.flink.auth;

import com.aliyun.openservices.log.common.auth.CredentialsProvider;

import java.io.Serializable;

/**
 * Serializable factory for creating an SLS credentials provider at runtime.
 *
 * <p>The factory is serialized with the Flink job, while the returned provider is created on the
 * JobManager or TaskManager and must not be serialized into the job graph or checkpoint state.
 */
@FunctionalInterface
public interface LogCredentialsProviderFactory extends Serializable {

    /**
     * Creates a credentials provider in the current Flink runtime process.
     *
     * @return a non-null SLS credentials provider
     */
    CredentialsProvider createCredentialsProvider();
}
