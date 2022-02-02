package com.aliyun.openservices.log.flink.internal;

import org.apache.flink.util.PropertiesUtil;

import java.io.Serializable;
import java.util.Properties;

public class ConfigParser implements Serializable {

    private final Properties props;

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
}
