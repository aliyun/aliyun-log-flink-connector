package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.internal.ConfigParser;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.http.client.ClientConfiguration;
import com.aliyun.openservices.log.http.signer.SignVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.PropertiesUtil;

import java.util.Properties;

import static com.aliyun.openservices.log.flink.ConfigConstants.REGION_ID;
import static com.aliyun.openservices.log.flink.ConfigConstants.SIGNATURE_VERSION;

public final class LogUtil {

    private LogUtil() {
    }

    public static CheckpointMode parseCheckpointMode(Properties properties) {
        final String mode = properties.getProperty(ConfigConstants.LOG_CHECKPOINT_MODE);
        if (mode == null || mode.isEmpty()) {
            return CheckpointMode.ON_CHECKPOINTS;
        }
        return CheckpointMode.fromString(mode);
    }

    public static long getDiscoveryIntervalMs(Properties props) {
        return PropertiesUtil.getLong(props,
                ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS,
                Consts.DEFAULT_SHARDS_DISCOVERY_INTERVAL_MILLIS);
    }

    public static long getCommitIntervalMs(Properties props) {
        return PropertiesUtil.getLong(props,
                ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS,
                Consts.DEFAULT_COMMIT_INTERVAL_MILLIS);
    }

    public static int getNumberPerFetch(Properties properties) {
        return PropertiesUtil.getInt(properties,
                ConfigConstants.LOG_MAX_NUMBER_PER_FETCH,
                Consts.DEFAULT_NUMBER_PER_FETCH);
    }

    public static long getFetchIntervalMillis(Properties properties) {
        return PropertiesUtil.getLong(properties,
                ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS,
                Consts.DEFAULT_FETCH_INTERVAL_MILLIS);
    }

    public static String getDefaultPosition(Properties properties) {
        final String val = properties.getProperty(ConfigConstants.LOG_CONSUMER_DEFAULT_POSITION);
        return val != null && !val.isEmpty() ? val : Consts.LOG_BEGIN_CURSOR;
    }

    private static SignVersion parseSignVersion(String signVersion) {
        for (SignVersion version : SignVersion.values()) {
            if (version.name().equalsIgnoreCase(signVersion)) {
                return version;
            }
        }
        return SignVersion.V1; // default v1
    }

    public static ClientConfiguration getClientConfiguration(ConfigParser parser) {
        SignVersion signVersion = LogUtil.parseSignVersion(parser.getString(SIGNATURE_VERSION));
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMaxConnections(com.aliyun.openservices.log.common.Consts.HTTP_CONNECT_MAX_COUNT);
        clientConfiguration.setConnectionTimeout(com.aliyun.openservices.log.common.Consts.HTTP_CONNECT_TIME_OUT);
        clientConfiguration.setSocketTimeout(com.aliyun.openservices.log.common.Consts.HTTP_SEND_TIME_OUT);
        clientConfiguration.setSignatureVersion(signVersion);
        if (signVersion == SignVersion.V4) {
            String regionId = parser.getString(REGION_ID);
            if (StringUtils.isBlank(regionId)) {
                throw new IllegalArgumentException("The " + REGION_ID + " was not specified for signature " + signVersion.name() + ".");
            }
            clientConfiguration.setRegion(regionId);
        }
        return clientConfiguration;
    }
}
