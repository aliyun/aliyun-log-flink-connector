# Flink log connector
## Introduction
The Flink log connector is a tool provided by Alibaba Cloud Log Service and used to connect to Flink. It consists of the consumer and producer.

The consumer reads data from Log Service. It supports the exactly-once syntax and shard-based load balancing.
The producer writes data into Log Service. When using the connector, you must add the Maven dependency to the project:
```
<dependency>
    <groupId>com.aliyun.openservices</groupId>
    <artifactId>flink-log-connector</artifactId>
    <version>0.1.16</version>
</dependency>
<dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>2.5.0</version>
</dependency>
```
## Usage
1. Create a LogStore. For details, see [Log Service documentation](~~54604~~).
2. If you use a subaccount for access, make sure that you have properly set the RAM policies of LogStore. For details, see [Authorize a RAM sub-user to access Log Service](~~47664~~).

### 1. Log Consumer
In the connector, the Flink log consumer provides the capability of subscribing to a specific LogStore in Log Service to achieve the exactly-once syntax. During use, you do not need to concern about the change of the number of shards in the LogStore.
The consumer automatically senses the change.

Each sub-task in Flink consumes some shards in the LogStore. If shards in the LogStore are split or merged, shards consumed by the sub-task change accordingly.
#### 1.1. Configure the startup parameters
```
Properties configProps = new Properties();
// Set the domain to access Log Service
configProps.put(ConfigConstants.LOG_ENDPOINT， "cn-hangzhou.log.aliyuncs.com");
// Set the AccessKey
configProps.put(ConfigConstants.LOG_ACCESSSKEYID， "");
configProps.put(ConfigConstants.LOG_ACCESSKEY， "");
// Set the Log Service project
configProps.put(ConfigConstants.LOG_PROJECT， "ali-cn-hangzhou-sls-admin");
// Set the Log Service LogStore
configProps.put(ConfigConstants.LOG_LOGSTORE， "sls_consumergroup_log");
// Set the start position to consume Log Service
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION， Consts.LOG_END_CURSOR);
// Set the message deserialization method for Log Service
RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<RawLogGroupList> logTestStream = env.addSource(
        new FlinkLogConsumer<RawLogGroupList>(deserializer， configProps));
```
The preceding is a simple consumption example. As java.util.Properties is used as the configuration tool, configurations of all consumers can be located in ConfigConstants.
> NOTE: The number of sub-tasks in the Flink stream is independent from that of shards in the Log Service LogStore. If the number of shards is greater than that of sub-tasks, each sub-task consumes multiple shards exactly once. If the number of shards is smaller than that of sub-tasks, some sub-tasks are idle until new shards are generated.

#### 1.2 Set the consumption start position
You can set the start position for consuming a shard on the Flink log consumer. By setting ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, you can set whether to consume a shard from its header or tail or at a specific time. The values are as follows:

* Consts.LOG_BEGIN_CURSOR: Indicates that the shard is consumed from its header, that is, from the earliest data of the shard.
* Consts.LOG_END_CURSOR: Indicates that the shard is consumed from its tail, that is, from the latest data of the shard.
* UnixTimestamp: A string of an integer value, which is expressed in seconds from 1970-01-01. It indicates that the shard is consumed from this time point.

Examples of the preceding three values are as follows:
```
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION， Consts.LOG_BEGIN_CURSOR);
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION， Consts.LOG_END_CURSOR);
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION， "1512439000");
```

#### 1.3 (Optional) Monitor the consumption progress
The Flink log consumer supports consumption progress monitoring. The consumption progress indicates the real-time consumption position of each shard, which is expressed using the timestamp. For details, see 
[ConsumerGroup - View status](~~43998~~)，[ConsumerGroup - Monitoring and alerting](~~55912~~).
```
configProps.put(ConfigConstants.LOG_CONSUMERGROUP， "your consumer group name”);
```
> NOTE: The preceding code is optional. If it is set, the consumer creates a ConsumerGroup first. If the ConsumerGroup already exists, no further operation is required. Snapshots in the consumer are automatically synchronized to the ConsumerGroup of Log Service. You can check the consumption progress of the consumer on the Log Service console.

#### 1.4 Support disaster tolerance and exactly-once syntax
If the checkpointing function is enabled on Flink, the Flink log consumer cyclically stores the consumption progress of each shard. When a job fails, Flink restores the log consumer 
and starts consumption from the latest checkpoint that is stored.

The write checkpoint cycle defines the maximum amount of data to be retrieved (re-consumed) if a failure occurs. The code is as follows:
```
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// Enable the exactly-once syntax on Flink
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// Store the checkpoint every 5s
env.enableCheckpointing(5000);
```
For details about the Flink checkpoints, see the Flink documentation [Checkpoints](https://ci.apache.org/projects/flink/flink-docs-release-1.3/setup/checkpoints.html).
#### 1.5 Supplement: Set the associated APIs and permissions
The Flink log consumer uses the following Alibaba Cloud Log Service APIs:
* GetCursorOrData

    This API is used to pull data from a shard. If this API is frequently called, data may exceed the shard quota of Log Service. You can use ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS and ConfigConstants.LOG_MAX_NUMBER_PER_FETCH 
    to control the interval of API calls and number of logs pulled by each call. For details about the shard quota, see [Shard overview](https://help.aliyun.com/document_detail/28976.html).
    ```
    configProps.put(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS， "100");
    configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH， "100");
    ```
* ListShards

     This API is used to obtain all shard lists and shard status in a LogStore. If your shards are always split and merged, you can adjust the API call cycle to locate shard changes in time.
     ```
     // Call ListShards every 30s
     configProps.put(ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS， "30000");
     ```
* CreateConsumerGroup

    This API is called only when consumption progress monitoring is enabled. It is used to create a ConsumerGroup to synchronize checkpoints.
* ConsumerGroupUpdateCheckPoint

    This API is used to synchronize snapshots of Flink to a ConsumerGroup of Log Service.

The following table lists the RAM authorization policies required for sub-users to use the Flink log consumer.

|API|Resource|
|------|------|
|log:GetCursorOrData| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|
|log:ListShards| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|
|log:CreateConsumerGroup| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}/consumergroup/*|
|log:ConsumerGroupUpdateCheckPoint|acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}/consumergroup/${consumerGroupName}|

### 2. Log Producer
The Flink log producer writes data into Alibaba Cloud Log Service.
> NOTE: The producer supports only the Flink at-least-once syntax. It means that when a job failure occurs, data written into Log Service may be duplicated but never lost.

The following uses writing a simulated string into Log Service as an example:
```
// Serialize data to the data format of Log Service
class SimpleLogSerializer implements LogSerializationSchema<String> {

    public RawLogGroup serialize(String element) {
        RawLogGroup rlg = new RawLogGroup();
        RawLog rl = new RawLog();
        rl.setTime((int)(System.currentTimeMillis() / 1000));
        rl.addContent("message"， element);
        rlg.addLog(rl);
        return rlg;
    }
}
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
        // Set the domain to access Log Service
        configProps.put(ConfigConstants.LOG_ENDPOINT， sEndpoint);
        // Set the AccessKey to access Log Service
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID， sAccessKeyId);
        configProps.put(ConfigConstants.LOG_ACCESSKEY， sAccessKey);
        // Set the Log Service project into which logs are written
        configProps.put(ConfigConstants.LOG_PROJECT， sProject);
        // Set the Log Service LogStore into which logs are written
        configProps.put(ConfigConstants.LOG_LOGSTORE， sLogstore);

        FlinkLogProducer<String> logProducer = new FlinkLogProducer<String>(new SimpleLogSerializer()， configProps);

        simpleStringStream.addSink(logProducer);

        env.execute("flink log producer");
    }
    // Simulate log generation
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
```
#### 2.1 Initialization
Perform the following two steps to initialize the producer:
* Initialize the configuration parameter Properties for the producer, which is similar to that for the consumer. The producer contains some custom parameters. Set these parameters to the default values in general conditions or to custom values as required.
    ```
    // The number of I/O threads used for sending data. The default value is 8
    ConfigConstants.LOG_SENDER_IO_THREAD_COUNT
    // The time when the log data is cached. The default value is 3000
    ConfigConstants.LOG_PACKAGE_TIMEOUT_MILLIS
    // The number of logs in the cached package. The default value is 4096
    ConfigConstants.LOG_LOGS_COUNT_PER_PACKAGE
    // The size of the cached package. The default value is 3Mb
    ConfigConstants.LOG_LOGS_BYTES_PER_PACKAGE
    // The total memory size that the job can use. The default value is 100Mb
    ConfigConstants.LOG_MEM_POOL_BYTES
    ```
    The preceding parameters are not mandatory. You can retain the default values.
* Reload LogSerializationSchema to define the method for serializing data to RawLogGroup.

    RawLogGroup is a collection of logs. For details about the meaning of each field, see [Log data model](~~29054~~).

To use the shardHashKey function of Log Service, specify the shard into which data is written. You can use LogPartitioner in the following way to generate the HashKey of data:
```
FlinkLogProducer<String> logProducer = new FlinkLogProducer<String>(new SimpleLogSerializer()， configProps);
logProducer.setCustomPartitioner(new LogPartitioner<String>() {
            // Generate a 32-bit hash value
            public String getHashKey(String element) {
                try {
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    md.update(element.getBytes());
                    String hash = new BigInteger(1， md.digest()).toString(16);
                    while(hash.length() < 32) hash = "0" + hash;
                    return hash;
                } catch (NoSuchAlgorithmException e) {
                }
                return  "0000000000000000000000000000000000000000000000000000000000000000";
            }
        });
```
> NOTE: LogPartitioner is optional. If this parameter is not set, data is randomly written into a shard.

#### 2.2 Permission setting: RAM policies
The producer uses the following APIs of Log Service to write data:

* log:PostLogStoreLogs
* log:ListShards

If a RAM sub-user uses the producer, the preceding two APIs must be authorized.

|API|Resource|
|------|------|
|log:PostLogStoreLogs| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|
|log:ListShards| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|
