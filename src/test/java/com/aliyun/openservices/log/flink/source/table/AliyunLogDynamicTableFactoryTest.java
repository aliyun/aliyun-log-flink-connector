package com.aliyun.openservices.log.flink.source.table;

import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;
import com.aliyun.openservices.log.flink.auth.ConfigurableLogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import org.apache.flink.table.api.ValidationException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AliyunLogDynamicTableFactoryTest {

    @Test
    public void testCredentialModesAreOptionalAndMutuallyValidatedAtCreation() {
        AliyunLogDynamicTableFactory factory = new AliyunLogDynamicTableFactory();

        assertFalse(factory.requiredOptions().contains(AliyunLogConnectorOptions.ACCESS_KEY_ID));
        assertFalse(factory.requiredOptions().contains(AliyunLogConnectorOptions.ACCESS_KEY));
        assertTrue(factory.optionalOptions().contains(AliyunLogConnectorOptions.ACCESS_KEY_ID));
        assertTrue(factory.optionalOptions().contains(AliyunLogConnectorOptions.ACCESS_KEY));
        assertTrue(factory.optionalOptions().contains(
                AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_FACTORY_CLASS));
    }

    @Test
    public void testStaticCredentialMode() {
        Map<String, String> options = new HashMap<>();
        options.put(AliyunLogConnectorOptions.ACCESS_KEY_ID.key(), "id");
        options.put(AliyunLogConnectorOptions.ACCESS_KEY.key(), "secret");

        CredentialsProvider provider = AliyunLogDynamicTableFactory
                .createCredentialsProviderFactory(options)
                .createCredentialsProvider();

        assertEquals("id", provider.getCredentials().getAccessKeyId());
        assertEquals("secret", provider.getCredentials().getAccessKeySecret());
    }

    @Test
    public void testDynamicCredentialModeWithProviderParameters() {
        Map<String, String> options = new HashMap<>();
        options.put(
                AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_FACTORY_CLASS.key(),
                ConfigurableTestFactory.class.getName());
        options.put(
                AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_PARAMETER_PREFIX + "roleArn",
                "test-role");

        CredentialsProvider provider = AliyunLogDynamicTableFactory
                .createCredentialsProviderFactory(options)
                .createCredentialsProvider();

        assertEquals("test-role", provider.getCredentials().getAccessKeyId());
    }

    @Test(expected = ValidationException.class)
    public void testRejectsMissingCredentialMode() {
        AliyunLogDynamicTableFactory.createCredentialsProviderFactory(new HashMap<>());
    }

    @Test(expected = ValidationException.class)
    public void testRejectsPartialStaticCredentials() {
        Map<String, String> options = new HashMap<>();
        options.put(AliyunLogConnectorOptions.ACCESS_KEY_ID.key(), "id");
        AliyunLogDynamicTableFactory.createCredentialsProviderFactory(options);
    }

    @Test(expected = ValidationException.class)
    public void testRejectsTwoCredentialModes() {
        Map<String, String> options = new HashMap<>();
        options.put(AliyunLogConnectorOptions.ACCESS_KEY_ID.key(), "id");
        options.put(AliyunLogConnectorOptions.ACCESS_KEY.key(), "secret");
        options.put(
                AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_FACTORY_CLASS.key(),
                ConfigurableTestFactory.class.getName());
        AliyunLogDynamicTableFactory.createCredentialsProviderFactory(options);
    }

    @Test(expected = ValidationException.class)
    public void testRejectsProviderParametersWithStaticCredentials() {
        Map<String, String> options = new HashMap<>();
        options.put(AliyunLogConnectorOptions.ACCESS_KEY_ID.key(), "id");
        options.put(AliyunLogConnectorOptions.ACCESS_KEY.key(), "secret");
        options.put(
                AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_PARAMETER_PREFIX + "roleArn",
                "test-role");
        AliyunLogDynamicTableFactory.createCredentialsProviderFactory(options);
    }

    public static class ConfigurableTestFactory
            implements ConfigurableLogCredentialsProviderFactory {
        private static final long serialVersionUID = 1L;

        private String roleArn;

        public ConfigurableTestFactory() {
        }

        @Override
        public void configure(Properties properties) {
            roleArn = properties.getProperty("roleArn");
        }

        @Override
        public CredentialsProvider createCredentialsProvider() {
            return new StaticCredentialsProvider(
                    new DefaultCredentials(roleArn, "dynamic-secret"));
        }
    }
}
