package com.aliyun.openservices.log.flink.sample;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogProducer;
import com.aliyun.openservices.log.flink.LogPartitioner;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

public class ProducerSample {
    private static final String SLS_ENDPOINT = "cn-hangzhou.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "";
    private static final String ACCESS_KEY = "";
    private static final String SLS_PROJECT = "";
    private static final String SLS_LOGSTORE = "test-flink-producer";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(3);
        DataStream<String> stream = env.addSource(new EventsGenerator());
        Properties configProps = new Properties();
        configProps.put(ConfigConstants.LOG_ENDPOINT, SLS_ENDPOINT);
        configProps.put(ConfigConstants.LOG_ACCESSKEYID, ACCESS_KEY_ID);
        configProps.put(ConfigConstants.LOG_ACCESSKEY, ACCESS_KEY);
        configProps.put(ConfigConstants.LOG_PROJECT, SLS_PROJECT);
        configProps.put(ConfigConstants.LOG_LOGSTORE, SLS_LOGSTORE);
        // Set max lines in one LogGroup
        configProps.put(ConfigConstants.LOG_GROUP_MAX_LINES, "5000");
        // Set max size of one logGroup to 3MB
        configProps.put(ConfigConstants.LOG_GROUP_MAX_SIZE, "3145728");

        FlinkLogProducer<String> logProducer = new FlinkLogProducer<>(new SimpleLogSerializer(), configProps);
        logProducer.setCustomPartitioner(new LogPartitioner<String>() {
            @Override
            public String getHashKey(String element) {
                String hash = "";
                try {
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    md.update(element.getBytes());
                    hash = new BigInteger(1, md.digest()).toString(16);
                } catch (NoSuchAlgorithmException ignore) {
                }
                StringBuilder builder = new StringBuilder();
                while (builder.length() < 32 - hash.length()) {
                    builder.append("0");
                }
                builder.append(hash);
                return builder.toString();
            }
        });
        stream.addSink(logProducer);
        env.execute("flink log producer");
    }

    public static class EventsGenerator implements SourceFunction<String> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long seq = 0;
            while (running) {
                Thread.sleep(10);
                ctx.collect((seq++) + "-" + RandomStringUtils.randomAlphabetic(12));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

