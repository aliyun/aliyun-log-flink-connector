package com.aliyun.openservices.log.flink.source;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.LogStore;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.flink.source.deserialization.AliyunLogDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AliyunLogSourceIntegrationTest {
    private static final String ENDPOINT_ENV = "ALIYUN_SLS_ENDPOINT";
    private static final String ACCESS_KEY_ID_ENV = "ALIYUN_ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET_ENV = "ALIYUN_ACCESS_KEY_SECRET";
    private static final int DEFAULT_RECORD_COUNT = 10_000;
    private static final int WRITE_BATCH_SIZE = 500;
    private static final int FAILOVER_AFTER_RECORDS = 6_000;

    @Test
    public void testCreateResourceWriteLogsAndReadWithFlip27Source() throws Exception {
        Assume.assumeTrue(Boolean.getBoolean("aliyun.sls.integration"));

        runIntegrationTest(false);
    }

    @Test
    public void testFailoverRestoresFromCheckpoint() throws Exception {
        Assume.assumeTrue(Boolean.getBoolean("aliyun.sls.integration"));

        File failureMarker = new File(System.getProperty("java.io.tmpdir"),
                "aliyun-log-source-failover-" + System.nanoTime());
        try {
            ReadResult result = runIntegrationTest(true, failureMarker.getAbsolutePath());
            assertTrue("The failover test did not trigger an intentional failure", failureMarker.exists());
            System.out.println("Failover test duplicate ids after recovery: " + result.duplicateCount);
        } finally {
            Files.deleteIfExists(failureMarker.toPath());
        }
    }

    private static ReadResult runIntegrationTest(boolean failover) throws Exception {
        return runIntegrationTest(failover, null);
    }

    private static ReadResult runIntegrationTest(boolean failover, String failureMarkerPath) throws Exception {
        String endpoint = requiredEnv(ENDPOINT_ENV);
        String accessKeyId = requiredEnv(ACCESS_KEY_ID_ENV);
        String accessKeySecret = requiredEnv(ACCESS_KEY_SECRET_ENV);

        String suffix = Long.toString(System.currentTimeMillis(), 36);
        String project = "flink-conn-it-" + suffix;
        String logstore = "it-logstore";
        String topic = failover ? "flip27-failover" : "flip27-smoke";

        Client client = new Client(endpoint, accessKeyId, accessKeySecret);
        try {
            createProject(client, project);
            waitUntilProjectReady(client, project);
            createLogstore(client, project, logstore);
            waitUntilLogstoreReady(client, project, logstore);

            List<String> expected = writeTestLogs(client, project, logstore, topic, recordCount());
            ReadResult result = runConnector(endpoint, accessKeyId, accessKeySecret,
                    project, logstore, expected.size(), failover, failureMarkerPath);
            assertEquals(new HashSet<>(expected), result.records);
            assertEquals("Connector emitted duplicate ids", 0, result.duplicateCount);
            return result;
        } finally {
            deleteLogstoreQuietly(client, project, logstore);
            deleteProjectQuietly(client, project);
            client.shutdown();
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        Assume.assumeTrue("Missing environment variable " + name, value != null && !value.isEmpty());
        return value;
    }

    private static void createProject(Client client, String project) throws LogException {
        client.CreateProject(project, "aliyun-log-flink-connector integration test");
        System.out.println("Created SLS project: " + project);
    }

    private static void createLogstore(Client client, String project, String logstore) throws LogException {
        LogStore store = new LogStore(logstore, 1, 1);
        client.CreateLogStore(project, store);
        System.out.println("Created SLS logstore: " + logstore);
    }

    private static int recordCount() {
        return Integer.getInteger("aliyun.sls.integration.records", DEFAULT_RECORD_COUNT);
    }

    private static List<String> writeTestLogs(
            Client client,
            String project,
            String logstore,
            String topic,
            int recordCount) throws Exception {
        List<String> expected = new ArrayList<>();
        List<LogItem> items = new ArrayList<>(WRITE_BATCH_SIZE);
        int now = (int) (System.currentTimeMillis() / 1000L);
        int written = 0;
        for (int i = 0; i < recordCount; i++) {
            String id = String.format("msg-%05d", i);
            expected.add(id);
            LogItem item = new LogItem(now);
            item.PushBack("id", id);
            item.PushBack("message", "connector integration " + i);
            item.PushBack("level", "INFO");
            items.add(item);
            if (items.size() == WRITE_BATCH_SIZE) {
                client.PutLogs(project, logstore, topic, items, "codex-integration");
                written += items.size();
                items.clear();
            }
        }
        if (!items.isEmpty()) {
            client.PutLogs(project, logstore, topic, items, "codex-integration");
            written += items.size();
        }
        System.out.println("Wrote " + written + " logs to SLS in batches of " + WRITE_BATCH_SIZE);
        return expected;
    }

    private static ReadResult runConnector(
            String endpoint,
            String accessKeyId,
            String accessKeySecret,
            String project,
            String logstore,
            int expectedCount,
            boolean failover,
            String failureMarkerPath) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.DISABLED.name());
        properties.setProperty(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "100");
        properties.setProperty(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, "200");
        properties.setProperty(ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS, "1000");

        AliyunLogSource<String> source = AliyunLogSource.<String>builder()
                .setProject(project)
                .setLogStore(logstore)
                .setEndpoint(endpoint)
                .setCredentials(accessKeyId, accessKeySecret)
                .setStartingPosition(StartingPosition.EARLIEST)
                .setProperties(properties)
                .setDeserializer(new IdDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        if (failover) {
            env.enableCheckpointing(500);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.seconds(1)));
        }
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "aliyun-log-it");
        if (failover) {
            stream = stream.map(new FailingOnceMap(FAILOVER_AFTER_RECORDS, failureMarkerPath));
        }

        Set<String> actual = new HashSet<>();
        int duplicateCount = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        try (CloseableIterator<String> iterator = stream.executeAndCollect()) {
            while (actual.size() < expectedCount && System.nanoTime() < deadline) {
                if (iterator.hasNext()) {
                    if (!actual.add(iterator.next())) {
                        duplicateCount++;
                    }
                }
            }
        }
        System.out.println("Connector read " + actual.size() + " unique ids, duplicates=" + duplicateCount
                + ", sample=" + sample(actual));
        return new ReadResult(actual, duplicateCount);
    }

    private static List<String> sample(Set<String> records) {
        List<String> sample = new ArrayList<>(5);
        for (String record : records) {
            sample.add(record);
            if (sample.size() == 5) {
                break;
            }
        }
        return sample;
    }

    private static void waitUntilProjectReady(Client client, String project) throws Exception {
        waitUntil(() -> client.GetProject(project), "project " + project);
    }

    private static void waitUntilLogstoreReady(Client client, String project, String logstore) throws Exception {
        waitUntil(() -> client.GetLogStore(project, logstore), "logstore " + logstore);
        waitUntil(() -> client.ListShard(project, logstore), "shards for " + logstore);
    }

    private static void waitUntil(CheckedRunnable runnable, String resource) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try {
                runnable.run();
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(2000L);
            }
        }
        throw new RuntimeException("Timed out waiting for " + resource, last);
    }

    private static void deleteLogstoreQuietly(Client client, String project, String logstore) {
        try {
            client.DeleteLogStore(project, logstore);
            System.out.println("Deleted SLS logstore: " + logstore);
        } catch (Exception e) {
            System.out.println("Failed to delete logstore " + logstore + ": " + e.getMessage());
        }
    }

    private static void deleteProjectQuietly(Client client, String project) {
        try {
            client.DeleteProject(project);
            System.out.println("Deleted SLS project: " + project);
        } catch (Exception e) {
            System.out.println("Failed to delete project " + project + ": " + e.getMessage());
        }
    }

    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private static class ReadResult {
        private final Set<String> records;
        private final int duplicateCount;

        private ReadResult(Set<String> records, int duplicateCount) {
            this.records = records;
            this.duplicateCount = duplicateCount;
        }
    }

    private static class IdDeserializationSchema implements AliyunLogDeserializationSchema<String> {
        @Override
        public void deserialize(PullLogsResult record, Collector<String> out) {
            for (LogGroupData logGroupData : record.getLogGroupList()) {
                FastLogGroup logGroup = logGroupData.GetFastLogGroup();
                for (int i = 0; i < logGroup.getLogsCount(); i++) {
                    FastLog log = logGroup.getLogs(i);
                    for (int j = 0; j < log.getContentsCount(); j++) {
                        FastLogContent content = log.getContents(j);
                        if ("id".equals(content.getKey())) {
                            out.collect(content.getValue());
                        }
                    }
                }
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static class FailingOnceMap extends RichMapFunction<String, String> {
        private final int failAfterRecords;
        private final String failureMarkerPath;
        private long seen;

        private FailingOnceMap(int failAfterRecords, String failureMarkerPath) {
            this.failAfterRecords = failAfterRecords;
            this.failureMarkerPath = failureMarkerPath;
        }

        @Override
        public String map(String value) throws Exception {
            seen++;
            if (seen <= failAfterRecords) {
                Thread.sleep(1L);
            }
            if (seen >= failAfterRecords && !Files.exists(Paths.get(failureMarkerPath))) {
                Files.write(Paths.get(failureMarkerPath), new byte[] {1});
                throw new RuntimeException("Intentional failure for source failover test");
            }
            return value;
        }
    }
}
