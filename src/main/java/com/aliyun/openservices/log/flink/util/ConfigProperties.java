package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.flink.ConfigConstants;

import java.util.Properties;

/** Utilities for copying connector properties at serialization boundaries. */
public final class ConfigProperties {

    private ConfigProperties() {
    }

    /**
     * Copies directly configured and inherited properties into an independent object.
     *
     * <p>{@link Properties#putAll(java.util.Map)} does not copy values inherited from the
     * defaults chain. Flattening {@link Properties#stringPropertyNames()} keeps those values
     * effective after the copy is serialized.
     *
     * @param source source properties, or null for an empty copy
     * @return independent properties containing all effective values
     */
    public static Properties copy(Properties source) {
        Properties copied = new Properties();
        if (source == null) {
            return copied;
        }
        copied.putAll(source);
        for (String propertyName : source.stringPropertyNames()) {
            if (!copied.containsKey(propertyName)) {
                copied.setProperty(propertyName, source.getProperty(propertyName));
            }
        }
        return copied;
    }

    /**
     * Returns an independent properties object without legacy static credentials.
     *
     * @param source source properties, or null for an empty copy
     * @return sanitized independent properties
     */
    public static Properties sanitizedCopyWithoutCredentials(Properties source) {
        Properties copied = copy(source);
        copied.remove(ConfigConstants.LOG_ACCESSKEYID);
        copied.remove(ConfigConstants.LOG_ACCESSKEY);
        return copied;
    }
}
