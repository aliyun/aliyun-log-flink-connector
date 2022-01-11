# Flink log connector
## 介绍
Flink log connector是阿里云日志服务提供的，用于对接flink的工具，包括两部分，消费者(Consumer)和生产者(Producer)。

消费者用于从日志服务中读取数据，支持exactly once语义，支持shard负载均衡.
生产者用于将数据写入日志服务，使用connector时，需要在项目中添加maven依赖：
```
<dependency>
    <groupId>com.aliyun.openservices</groupId>
    <artifactId>flink-log-connector</artifactId>
    <version>0.1.31</version>
</dependency>
<dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>2.5.0</version>
</dependency>
```
## 用法
1. 请参考[日志服务文档](https://help.aliyun.com/document_detail/54604.html)，正确创建Logstore。
2. 如果使用子账号访问，请确认正确设置了LogStore的RAM策略。参考[授权RAM子用户访问日志服务资源](https://help.aliyun.com/document_detail/47664.html)。

### 1. Log consumer
在Connector中， 类FlinkLogConsumer提供了订阅日志服务中某一个LogStore的能力，实现了exactly once语义，在使用时，用户无需关心LogStore中shard数
量的变化，consumer会自动感知。

flink中每一个子任务负责消费LogStore中部分shard，如果LogStore中shard发生split或者merge，子任务消费的shard也会随之改变。
#### 1.1 配置启动参数
```
Properties configProps = new Properties();
// 设置访问日志服务的域名
configProps.put(ConfigConstants.LOG_ENDPOINT， "cn-hangzhou.log.aliyuncs.com");
// 设置访问ak
configProps.put(ConfigConstants.LOG_ACCESSSKEYID， "");
configProps.put(ConfigConstants.LOG_ACCESSKEY， "");
// 设置日志服务的project
configProps.put(ConfigConstants.LOG_PROJECT， "ali-cn-hangzhou-sls-admin");
// 设置日志服务的LogStore
configProps.put(ConfigConstants.LOG_LOGSTORE， "sls_consumergroup_log");
// 设置消费日志服务起始位置
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION， Consts.LOG_END_CURSOR);
// 设置每次读取的LogGroup个数，默认100
configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "100");
// 设置日志服务的消息反序列化方法
RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<RawLogGroupList> logTestStream = env.addSource(
        new FlinkLogConsumer<RawLogGroupList>(deserializer， configProps));
```
上面是一个简单的消费示例，我们使用java.util.Properties作为配置工具，所有Consumer的配置都可以在ConfigConstants中找到。
> 注意，Flink Task 数量和日志服务LogStore中的shard数量是独立的，如果shard数量多于子任务数量，每个子任务不重复的消费多个shard，如果少于，
那么部分子任务就会空闲，除非新的shard产生。
#### 1.2 设置消费起始位置
Flink log consumer支持设置shard的消费起始位置，通过设置属性ConfigConstants.LOG_CONSUMER_BEGIN_POSITION，就可以定制消费从shard的头尾或者某个特定时间开始消费，具体取值如下：

* Consts.LOG_BEGIN_CURSOR： 表示从shard的头开始消费，也就是从shard中最旧的数据开始消费。
* Consts.LOG_END_CURSOR： 表示从shard的尾开始，也就是从shard中最新的数据开始消费。
* Consts.LOG_FROM_CHECKPOINT：以消费组的checkpoint作为消费的起始位置。
* Unix 时间戳： 一个整型数值的字符串，用1970-01-01到现在的秒数表示，含义是消费shard中这个时间点开始写入的数据。

四种取值举例如下：
```
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION，Consts.LOG_BEGIN_CURSOR);
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION，Consts.LOG_END_CURSOR);
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION，Consts.LOG_FROM_CHECKPOINT);
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION，"1512439000");
```
当从消费的checkpoint获取起始位置时，必须提供消费组名称。除此之外还支持设置一个默认的起始位置，在消费组不存在或者无法从消费组
中获取checkpoint时，将自动切换到默认的起始位置。如下所示：
```
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION，Consts.LOG_FROM_CHECKPOINT);
configProps.put(ConfigConstants.LOG_CONSUMER_DEFAULT_POSITION，Consts.LOG_END_CURSOR);
```
> 注意: 默认的位置不支持设置为```Consts.LOG_FROM_CHECKPOINT```且默认值为```Consts.LOG_BEGIN_CURSOR```。

#### 1.3 监控：消费进度(可选)
Flink log consumer支持设置消费进度监控，所谓消费进度就是获取每一个shard实时的消费位置，这个位置使用时间戳表示，详细概念可以参考
文档[消费组-查看状态](https://help.aliyun.com/document_detail/43998.html)，[消费组-监控报警
](https://help.aliyun.com/document_detail/55912.html)。
```
configProps.put(ConfigConstants.LOG_CONSUMERGROUP， "your consumer group name”);
```
> 注意上面代码是可选的，如果设置了，consumer会首先创建consumerGroup，如果已经存在，则什么都不做，consumer中的snapshot会自动同步到日志服务的consumerGroup中，用户可以在日志服务的控制台查看consumer的消费进度。
#### 1.4 消费组Checkpoint 提交模式

通过配置LOG_CHECKPOINT_MODE这个参数可以指定消费组Checkpoint的提交模式，目前支持如下三种：
```
configProps.put(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.PERIODIC.name());
configProps.put(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.ON_CHECKPOINTS.name());
configProps.put(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.DISABLED.name());
```
默认为 ON_CHECKPOINTS。

* ON_CHECKPOINT

选择 ON_CHECKPOINTS 时，当打开Flink的Checkpointing功能时，每个Shard的消费进度会保存在Flink的State中，同时会提交到日志服务服务端，当作业Failover时，会从Flink的State中恢复，如果不存在对应的checkpoint，会从服务端保存的最新的checkpoint恢复。

写checkpoint的周期定义了当发生失败时，最多多少的数据会被重复消费，Flink设置Checkpointing代码如下：
```
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 开启flink exactly once语义
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 每5s保存一次checkpoint
env.enableCheckpointing(5000);
```
更多Flink checkpoint的细节请参考Flink官方文档[Checkpoints](https://ci.apache.org/projects/flink/flink-docs-release-1.3/setup/checkpoints.html)。

* DISABLED
选择DISABLED时，checkpoint不会被提交到日志服务服务端。

* PERIODIC
选择PERIODIC时，checkpoint被定时提交到日志服务服务端，和Flink Checkpointing完全独立。支持自定义提交间隔：
```
configProps.put(ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS, "1000");
```
默认为10秒提交一次。

> 注意: 这个配置只和消费组提交checkpoint到日志服务有关，无论这个配置如何配置，都不影响Flink的State。

#### 1.5 补充材料：关联 API与权限设置
Flink log consumer 会用到的阿里云日志服务接口如下：
* GetCursorOrData

    用于从shard中拉数据， 注意频繁的调用该接口可能会导致数据超过日志服务的shard quota， 可以通过ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS和ConfigConstants.LOG_MAX_NUMBER_PER_FETCH
    控制接口调用的时间间隔和每次调用拉取的日志数量，shard的quota参考文章[shard简介](https://help.aliyun.com/document_detail/28976.html).
    ```
    configProps.put(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS， "100");
    configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH， "100");
    ```
* ListShards

     用于获取logStore中所有的shard列表，获取shard状态等.如果您的shard经常发生分裂合并，可以通过调整接口的调用周期来及时发现shard的变化。
     ```
     // 设置每30s调用一次ListShards
     configProps.put(ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS， "30000");
     ```
* CreateConsumerGroup

    该接口调用只有当设置消费进度监控时才会发生，功能是创建consumerGroup，用于同步checkpoint。
* ConsumerGroupUpdateCheckPoint

    该接口用户将flink的snapshot同步到日志服务的consumerGroup中。

子用户使用Flink log consumer需要授权如下几个RAM Policy：

|接口|资源|
|------|------|
|log:GetCursorOrData| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|
|log:ListShards| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|
|log:CreateConsumerGroup| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}/consumergroup/*|
|log:ConsumerGroupUpdateCheckPoint|acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}/consumergroup/${consumerGroupName}|

### 2. Log producer
FlinkLogProducer 用于将数据写到阿里云日志服务中。
> 注意 producer只支持Flink at-least-once语义，这就意味着在发生作业失败的情况下，写入日志服务中的数据有可能会重复，但是绝对不会丢失。

用法示例如下，我们将模拟产生的字符串写入日志服务：
```
// 将数据序列化成日志服务的数据格式
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
        // 设置访问日志服务的域名
        configProps.put(ConfigConstants.LOG_ENDPOINT， sEndpoint);
        // 设置访问日志服务的ak
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID， sAccessKeyId);
        configProps.put(ConfigConstants.LOG_ACCESSKEY， sAccessKey);
        // 设置日志写入的日志服务project
        configProps.put(ConfigConstants.LOG_PROJECT， sProject);
        // 设置日志写入的日志服务logStore
        configProps.put(ConfigConstants.LOG_LOGSTORE， sLogstore);

        FlinkLogProducer<String> logProducer = new FlinkLogProducer<String>(new SimpleLogSerializer()， configProps);

        simpleStringStream.addSink(logProducer);

        env.execute("flink log producer");
    }
    // 模拟产生日志
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
#### 2.1 初始化
Producer初始化主要需要做两件事情：
* 初始化配置参数Properties， 这一步和Consumer类似， Producer有一些定制的参数，一般情况下使用默认值即可，特殊场景可以考虑定制：
    ```
    // 用于发送数据的io线程的数量，默认是8
    ConfigConstants.LOG_SENDER_IO_THREAD_COUNT
    // 该值定义日志数据被缓存发送的时间，默认是3000
    ConfigConstants.LOG_PACKAGE_TIMEOUT_MILLIS
    // 缓存发送的包中日志的数量，默认是4096
    ConfigConstants.LOG_LOGS_COUNT_PER_PACKAGE
    // 缓存发送的包的大小，默认是3Mb
    ConfigConstants.LOG_LOGS_BYTES_PER_PACKAGE
    // 作业可以使用的内存总的大小，默认是100Mb
    ConfigConstants.LOG_MEM_POOL_BYTES
    ```
    上述参数不是必选参数，用户可以不设置，直接使用默认值。
* 重载LogSerializationSchema，定义将数据序列化成RawLogGroup的方法。

    RawLogGroup是log的集合，每个字段的含义可以参考文档[日志数据模型](https://help.aliyun.com/document_detail/29054.html)。

如果用户需要使用日志服务的shardHashKey功能，指定数据写到某一个shard中，可以使用LogPartitioner产生数据的hashKey，用法例子如下：
```
FlinkLogProducer<String> logProducer = new FlinkLogProducer<String>(new SimpleLogSerializer()， configProps);
logProducer.setCustomPartitioner(new LogPartitioner<String>() {
            // 生成32位hash值
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
> 注意LogPartitioner是可选的，不设置情况下， 数据会随机写入某一个shard。

#### 2.2 权限设置：RAM Policy
Producer依赖日志服务的API写数据，如下：

* log:PostLogStoreLogs
* log:ListShards

当RAM子用户使用Producer时，需要对上述两个API进行授权：

|接口|资源|
|------|------|
|log:PostLogStoreLogs| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|