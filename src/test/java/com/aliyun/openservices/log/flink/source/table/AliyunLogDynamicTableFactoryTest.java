package com.aliyun.openservices.log.flink.source.table;

import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.auth.ConfigurableLogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.StaticCredentialsProviderFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void testStaticSqlSourceUsesUnifiedCredentialFactory() throws Exception {
        Map<String, String> options = baseOptions();

        DynamicTableSource source = new AliyunLogDynamicTableFactory()
                .createDynamicTableSource(createContext(options));

        assertTrue(source instanceof AliyunLogDynamicSource);
        assertTrue(getField(source, "credentialsProviderFactory")
                instanceof StaticCredentialsProviderFactory);
    }

    @Test
    public void testUnifiedCredentialConstructorSanitizesLegacyProperties() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConfigConstants.LOG_ACCESSKEYID, "legacy-id");
        properties.setProperty(ConfigConstants.LOG_ACCESSKEY, "legacy-secret");
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("message", DataTypes.STRING())).getLogicalType();

        AliyunLogDynamicSource source = new AliyunLogDynamicSource(
                "project",
                "logstore",
                "endpoint",
                new PlanningOnlyFactory(),
                properties,
                rowType,
                false,
                null);

        Properties storedProperties = (Properties) getField(source, "properties");
        assertFalse(storedProperties.containsKey(ConfigConstants.LOG_ACCESSKEYID));
        assertFalse(storedProperties.containsKey(ConfigConstants.LOG_ACCESSKEY));
    }

    @Test
    public void testSqlPlanningValidatesFactoryWithoutInstantiatingIt() {
        PlanningOnlyFactory.CREATED.set(0);
        Map<String, String> options = dynamicOptions(PlanningOnlyFactory.class.getName());

        new AliyunLogDynamicTableFactory()
                .createDynamicTableSource(createContext(options));

        assertEquals(0, PlanningOnlyFactory.CREATED.get());
    }

    @Test
    public void testSqlSinkPlanningValidatesFactoryWithoutInstantiatingIt() {
        PlanningOnlyFactory.CREATED.set(0);
        Map<String, String> options = dynamicOptions(PlanningOnlyFactory.class.getName());

        DynamicTableSink sink = new AliyunLogDynamicTableFactory()
                .createDynamicTableSink(createContext(options));

        assertTrue(sink instanceof AliyunLogDynamicSink);
        assertEquals(0, PlanningOnlyFactory.CREATED.get());
    }

    @Test(expected = ValidationException.class)
    public void testSqlPlanningRejectsFactoryWithWrongType() {
        new AliyunLogDynamicTableFactory()
                .createDynamicTableSource(createContext(dynamicOptions(String.class.getName())));
    }

    @Test(expected = ValidationException.class)
    public void testSqlPlanningRejectsMissingFactoryClass() {
        new AliyunLogDynamicTableFactory()
                .createDynamicTableSource(createContext(
                        dynamicOptions("com.example.MissingCredentialsProviderFactory")));
    }

    @Test(expected = ValidationException.class)
    public void testSqlPlanningRejectsFactoryWithoutNoArgConstructor() {
        new AliyunLogDynamicTableFactory()
                .createDynamicTableSource(createContext(
                        dynamicOptions(FactoryWithoutNoArgConstructor.class.getName())));
    }

    @Test(expected = ValidationException.class)
    public void testSqlPlanningRejectsParametersForNonConfigurableFactory() {
        Map<String, String> options = dynamicOptions(PlanningOnlyFactory.class.getName());
        options.put(
                AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_PARAMETER_PREFIX + "roleArn",
                "test-role");
        new AliyunLogDynamicTableFactory()
                .createDynamicTableSource(createContext(options));
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

    public static class PlanningOnlyFactory implements LogCredentialsProviderFactory {
        private static final long serialVersionUID = 1L;
        private static final AtomicInteger CREATED = new AtomicInteger();

        public PlanningOnlyFactory() {
            CREATED.incrementAndGet();
        }

        @Override
        public CredentialsProvider createCredentialsProvider() {
            return new StaticCredentialsProvider(new DefaultCredentials("id", "secret"));
        }
    }

    public static class FactoryWithoutNoArgConstructor
            implements LogCredentialsProviderFactory {
        private static final long serialVersionUID = 1L;

        public FactoryWithoutNoArgConstructor(String ignored) {
        }

        @Override
        public CredentialsProvider createCredentialsProvider() {
            return new StaticCredentialsProvider(new DefaultCredentials("id", "secret"));
        }
    }

    private static Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", AliyunLogConnectorOptions.IDENTIFIER);
        options.put(AliyunLogConnectorOptions.ENDPOINT.key(), "endpoint");
        options.put(AliyunLogConnectorOptions.PROJECT.key(), "project");
        options.put(AliyunLogConnectorOptions.LOGSTORE.key(), "logstore");
        options.put(AliyunLogConnectorOptions.ACCESS_KEY_ID.key(), "id");
        options.put(AliyunLogConnectorOptions.ACCESS_KEY.key(), "secret");
        return options;
    }

    private static Map<String, String> dynamicOptions(String factoryClassName) {
        Map<String, String> options = baseOptions();
        options.remove(AliyunLogConnectorOptions.ACCESS_KEY_ID.key());
        options.remove(AliyunLogConnectorOptions.ACCESS_KEY.key());
        options.put(
                AliyunLogConnectorOptions.CREDENTIALS_PROVIDER_FACTORY_CLASS.key(),
                factoryClassName);
        return options;
    }

    private static DynamicTableFactory.Context createContext(Map<String, String> options) {
        ResolvedSchema resolvedSchema = new ResolvedSchema(
                Collections.singletonList(Column.physical("message", DataTypes.STRING())),
                Collections.emptyList(),
                (UniqueConstraint) null);
        CatalogTable catalogTable = CatalogTable.of(
                Schema.newBuilder().column("message", DataTypes.STRING()).build(),
                null,
                Collections.emptyList(),
                options);
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);
        return new DynamicTableFactory.Context() {
            @Override
            public ObjectIdentifier getObjectIdentifier() {
                return ObjectIdentifier.of("catalog", "database", "table");
            }

            @Override
            public ResolvedCatalogTable getCatalogTable() {
                return resolvedCatalogTable;
            }

            @Override
            public Configuration getConfiguration() {
                return new Configuration();
            }

            @Override
            public ClassLoader getClassLoader() {
                return getClass().getClassLoader();
            }

            @Override
            public boolean isTemporary() {
                return false;
            }
        };
    }

    private static Object getField(Object value, String name) throws Exception {
        Field field = value.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(value);
    }
}
