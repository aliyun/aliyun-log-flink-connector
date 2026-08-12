package com.aliyun.openservices.log.flink.auth;

import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;

/** Creates the SLS SDK provider used by the connector's existing static credential mode. */
public final class StaticCredentialsProviderFactory implements LogCredentialsProviderFactory {
    private static final long serialVersionUID = 1L;

    private final String accessKeyId;
    private final String accessKeySecret;

    public StaticCredentialsProviderFactory(String accessKeyId, String accessKeySecret) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
    }

    @Override
    public CredentialsProvider createCredentialsProvider() {
        return new StaticCredentialsProvider(
                new DefaultCredentials(accessKeyId, accessKeySecret));
    }
}
