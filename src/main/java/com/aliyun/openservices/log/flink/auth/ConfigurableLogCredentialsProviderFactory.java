package com.aliyun.openservices.log.flink.auth;

import java.util.Properties;

/**
 * A credentials provider factory that can be initialized from Flink SQL connector options.
 *
 * <p>Implementations used by SQL must provide a public no-argument constructor. The properties
 * passed to {@link #configure(Properties)} contain only keys below the
 * {@code credentials.provider.param.} prefix, with that prefix removed.
 */
public interface ConfigurableLogCredentialsProviderFactory extends LogCredentialsProviderFactory {

    /**
     * Configures this factory before it creates a credentials provider.
     *
     * @param properties provider-specific, non-secret configuration
     */
    void configure(Properties properties);
}
