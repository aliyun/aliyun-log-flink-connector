package com.aliyun.openservices.log.flink.internal;

import org.apache.flink.util.PropertiesUtil;

import java.io.Serializable;
import java.util.Properties;

public class ConfigWrapper implements Serializable {

    private Properties props;

    public ConfigWrapper(Properties props) {
        this.props = props;
    }

    public int getInt(String key, int defaultValue) {
        return PropertiesUtil.getInt(props, key, defaultValue);
    }

    public long getLong(String key, long defaultValue) {
        return PropertiesUtil.getLong(props, key, defaultValue);
    }

    public String getString(String key) {
        return props.getProperty(key);
    }
}
