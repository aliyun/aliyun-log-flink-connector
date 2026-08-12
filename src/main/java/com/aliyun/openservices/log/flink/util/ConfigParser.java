package com.aliyun.openservices.log.flink.util;

import org.apache.flink.util.PropertiesUtil;

import java.io.Serializable;
import java.util.Properties;

public class ConfigParser implements Serializable {

    private static final long serialVersionUID = -719303713679992247L;

    private Properties props;
    private boolean ownsProperties;

    public ConfigParser(Properties props) {
        this.props = props;
    }

    public int getInt(String key, int defaultValue) {
        return PropertiesUtil.getInt(props, key, defaultValue);
    }

    public long getLong(String key, long defaultValue) {
        return PropertiesUtil.getLong(props, key, defaultValue);
    }

    public boolean getBool(String key, boolean defaultValue) {
        return PropertiesUtil.getBoolean(props, key, defaultValue);
    }

    public String getString(String key) {
        return props.getProperty(key);
    }

    public void remove(String key) {
        if (!ownsProperties) {
            Properties copied = new Properties();
            copied.putAll(props);
            for (String propertyName : props.stringPropertyNames()) {
                if (!copied.containsKey(propertyName)) {
                    copied.setProperty(propertyName, props.getProperty(propertyName));
                }
            }
            props = copied;
            ownsProperties = true;
        }
        props.remove(key);
    }
}
