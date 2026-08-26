package com.aliyun.openservices.log.flink.auth;

import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.flink.util.ConfigProperties;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
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
        this.properties = ConfigProperties.copy(properties);
    }

    @Override
    public CredentialsProvider createCredentialsProvider() {
        LogCredentialsProviderFactory factory = instantiateFactory();
        if (factory instanceof ConfigurableLogCredentialsProviderFactory) {
            ((ConfigurableLogCredentialsProviderFactory) factory)
                    .configure(ConfigProperties.copy(properties));
        }
        CredentialsProvider provider = factory.createCredentialsProvider();
        if (provider == null) {
            throw new IllegalStateException(
                    "Credentials provider factory " + factoryClassName + " returned null");
        }
        return provider;
    }

    /**
     * Validates the application factory without instantiating it or creating credentials.
     *
     * <p>Flink SQL calls this during table creation so invalid classes fail during planning
     * rather than after the job has been deployed.
     *
     * @param classLoader user-code class loader used by Flink SQL
     */
    public void validateFactoryClass(ClassLoader classLoader) {
        try {
            validateFactoryClass(loadFactoryClass(classLoader, false));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new IllegalArgumentException(
                    "Invalid credentials provider factory " + factoryClassName,
                    e);
        }
    }

    private LogCredentialsProviderFactory instantiateFactory() {
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader classLoader = contextClassLoader != null
                    ? contextClassLoader
                    : ReflectiveLogCredentialsProviderFactory.class.getClassLoader();
            Class<?> factoryClass = loadFactoryClass(classLoader, true);
            Constructor<?> constructor = validateFactoryClass(factoryClass);
            return (LogCredentialsProviderFactory) constructor.newInstance();
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate credentials provider factory " + factoryClassName,
                    e);
        }
    }

    private Class<?> loadFactoryClass(ClassLoader classLoader, boolean initialize)
            throws ClassNotFoundException {
        ClassLoader effectiveClassLoader = classLoader != null
                ? classLoader
                : ReflectiveLogCredentialsProviderFactory.class.getClassLoader();
        return Class.forName(factoryClassName, initialize, effectiveClassLoader);
    }

    private Constructor<?> validateFactoryClass(Class<?> factoryClass) {
        if (!LogCredentialsProviderFactory.class.isAssignableFrom(factoryClass)) {
            throw new IllegalArgumentException(
                    "Credentials provider factory " + factoryClassName
                            + " does not implement LogCredentialsProviderFactory");
        }
        if (!properties.isEmpty()
                && !ConfigurableLogCredentialsProviderFactory.class.isAssignableFrom(factoryClass)) {
            throw new IllegalArgumentException(
                    "Credentials provider factory " + factoryClassName
                            + " must implement ConfigurableLogCredentialsProviderFactory "
                            + "when provider parameters are configured");
        }
        if (!Modifier.isPublic(factoryClass.getModifiers())
                || Modifier.isAbstract(factoryClass.getModifiers())) {
            throw new IllegalArgumentException(
                    "Credentials provider factory " + factoryClassName
                            + " must be a public concrete class");
        }
        Constructor<?> constructor;
        try {
            constructor = factoryClass.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "Credentials provider factory " + factoryClassName
                            + " must have a public no-argument constructor",
                    e);
        }
        if (!Modifier.isPublic(constructor.getModifiers())) {
            throw new IllegalArgumentException(
                    "Credentials provider factory " + factoryClassName
                            + " must have a public no-argument constructor");
        }
        return constructor;
    }
}
