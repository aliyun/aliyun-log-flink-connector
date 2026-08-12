package com.aliyun.openservices.log.flink.auth;

import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;

/** Creates the SLS SDK provider used by the connector's existing static credential mode. */
public final class StaticCredentialsProviderFactory implements LogCredentialsProviderFactory {
    private static final long serialVersionUID = 1L;

    private final String accessKeyId;
    private final String accessKeySecret;
    private final String securityToken;

    public StaticCredentialsProviderFactory(String accessKeyId, String accessKeySecret) {
        this(accessKeyId, accessKeySecret, null);
    }

    public StaticCredentialsProviderFactory(
            String accessKeyId,
            String accessKeySecret,
            String securityToken) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.securityToken = securityToken;
    }

    @Override
    public CredentialsProvider createCredentialsProvider() {
        if (securityToken == null || securityToken.isEmpty()) {
            return new StaticCredentialsProvider(
                    new DefaultCredentials(accessKeyId, accessKeySecret));
        }
        return new StaticCredentialsProvider(
                new DefaultCredentials(accessKeyId, accessKeySecret, securityToken));
    }
}
