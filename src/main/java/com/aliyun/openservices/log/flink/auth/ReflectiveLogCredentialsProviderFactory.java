package com.aliyun.openservices.log.flink.auth;

import com.aliyun.openservices.log.common.auth.CredentialsProvider;

import java.util.Properties;

/**
 * Runtime bridge used by Flink SQL to load an application-provided credentials factory by class
 * name without adding a dependency from the connector to a concrete credential implementation.
 */
public final class ReflectiveLogCredentialsProviderFactory
        implements LogCredentialsProviderFactory {
    private static final long serialVersionUID = 1L;

    private final String factoryClassName;
    private final Properties properties;

    public ReflectiveLogCredentialsProviderFactory(
            String factoryClassName,
            Properties properties) {
        if (factoryClassName == null || factoryClassName.trim().isEmpty()) {
            throw new IllegalArgumentException("Credentials provider factory class must be set");
        }
        this.factoryClassName = factoryClassName.trim();
        this.properties = copyProperties(properties);
    }

    @Override
    public CredentialsProvider createCredentialsProvider() {
        LogCredentialsProviderFactory factory = instantiateFactory();
        if (factory instanceof ConfigurableLogCredentialsProviderFactory) {
            ((ConfigurableLogCredentialsProviderFactory) factory)
                    .configure(copyProperties(properties));
        } else if (!properties.isEmpty()) {
            throw new IllegalArgumentException(
                    "Credentials provider factory " + factoryClassName
                            + " must implement ConfigurableLogCredentialsProviderFactory "
                            + "when provider parameters are configured");
        }
        CredentialsProvider provider = factory.createCredentialsProvider();
        if (provider == null) {
            throw new IllegalStateException(
                    "Credentials provider factory " + factoryClassName + " returned null");
        }
        return provider;
    }

    private LogCredentialsProviderFactory instantiateFactory() {
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader classLoader = contextClassLoader != null
                    ? contextClassLoader
                    : ReflectiveLogCredentialsProviderFactory.class.getClassLoader();
            Class<?> factoryClass = Class.forName(factoryClassName, true, classLoader);
            if (!LogCredentialsProviderFactory.class.isAssignableFrom(factoryClass)) {
                throw new IllegalArgumentException(
                        "Credentials provider factory " + factoryClassName
                                + " does not implement LogCredentialsProviderFactory");
            }
            return (LogCredentialsProviderFactory) factoryClass.getDeclaredConstructor().newInstance();
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate credentials provider factory " + factoryClassName,
                    e);
        }
    }

    private static Properties copyProperties(Properties source) {
        Properties copy = new Properties();
        if (source != null) {
            copy.putAll(source);
        }
        return copy;
    }
}
