package org.apache.flink.streaming.connectors.log.sample;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.log.ConfigConstants;
import org.apache.flink.streaming.connectors.log.FlinkLogProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerSample {
    public static String sEndpoint = "cn-hangzhou.log.aliyuncs.com";
    public static String sAccessKeyId = "";
    public static String sAccessKey = "";
    public static String sProject = "ali-cn-hangzhou-sls-admin";
    public static String sLogstore = "test-flink-producer";
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerSample.class);


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(3);

        DataStream<String> simpleStringStream = env.addSource(new EventsGenerator());

        Properties configProps = new Properties();
        configProps.put(ConfigConstants.LOG_ENDPOINT, sEndpoint);
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, sAccessKeyId);
        configProps.put(ConfigConstants.LOG_ACCESSKEY, sAccessKey);
        configProps.put(ConfigConstants.LOG_PROJECT, sProject);
        configProps.put(ConfigConstants.LOG_LOGSTORE, sLogstore);

        FlinkLogProducer<String> logProducer = new FlinkLogProducer<String>(new SimpleLogSerializer(), configProps);

        simpleStringStream.addSink(logProducer);

        env.execute("flink log producer");
    }
    public static class EventsGenerator implements SourceFunction<String> {
        private boolean running = true;

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

