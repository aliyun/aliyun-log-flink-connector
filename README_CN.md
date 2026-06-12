# Flink log connector
## 介绍
Flink log connector是阿里云日志服务提供的，用于对接flink的工具，包括两部分，消费者(Consumer)和生产者(Producer)。

消费者用于从日志服务中读取数据，支持exactly once语义，支持shard负载均衡.
生产者用于将数据写入日志服务，使用connector时，需要在项目中添加maven依赖：
```
<dependency>
    <groupId>com.aliyun.openservices</groupId>
    <artifactId>flink-log-connector</artifactId>
    <version>0.1.44</version>
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

### 1. FlinkLogConsumer
FlinkLogConsumer 提供了订阅日志服务中某一个Project 中单个或者多个 LogStore的能力，支持 Exactly Once 语义，在使用时，用户无需关心LogStore中shard数
量的变化，FlinkLogConsumer 会自动感知。

Flink中每一个子任务负责消费LogStore中部分shard，如果LogStore中shard发生split或者merge，子任务消费的shard也会随之改变。shard 和任务之间的默认分配关系为
```
shard id % task 个数 == 分配的task id
```

也可以通过设置自定义分配策略来实现个性化的分配方式。

#### 1.1 配置启动参数
```
Properties configProps = new Properties();
// 设置访问日志服务的域名
configProps.put(ConfigConstants.LOG_ENDPOINT， "cn-hangzhou.log.aliyuncs.com");
// 设置访问ak
configProps.put(ConfigConstants.LOG_ACCESSKEYID， "");
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
FlinkLogConsumer 支持设置消费进度监控，所谓消费进度就是获取每一个shard实时的消费位置，这个位置使用时间戳表示，详细概念可以参考
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

* ON_CHECKPOINTS

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

#### 1.5 SQL Connector

FLIP-27 Source 同时支持 Flink SQL，connector 标识为 `aliyun-log`。每条 SLS log 会展开成一行，普通列按同名 log content 取值；保留列名 `__time__`、`__topic__`、`__source__`、`__shard__`、`__cursor__` 可用于读取内置元数据。

```sql
CREATE TABLE sls_logs (
  `__time__` TIMESTAMP(3),
  `__topic__` STRING,
  `__source__` STRING,
  level STRING,
  message STRING,
  status_code INT
) WITH (
  'connector' = 'aliyun-log',
  'endpoint' = 'cn-hangzhou.log.aliyuncs.com',
  'project' = 'your-project',
  'logstore' = 'your-logstore',
  'access-key-id' = '${ACCESS_KEY_ID}',
  'access-key' = '${ACCESS_KEY_SECRET}',
  'consumer-group' = 'flink-sql-consumer',
  'scan.startup.mode' = 'checkpoint',
  'scan.startup.default-position' = 'earliest',
  'checkpoint.mode' = 'on-checkpoints',
  'max.number.per.fetch' = '100',
  'shards.discovery.interval.ms' = '60000',
  'ignore-parse-errors' = 'true'
);
```

#### 1.6 参数说明

DataStream API 使用 `ConfigConstants` 中的配置 key。SQL Connector 使用 `WITH` 参数，内部会映射到同一套 Source 配置。除特别说明外，时间单位均为毫秒，Unix 时间戳单位为秒。

##### 1.6.1 Source 通用参数

| DataStream 参数 | SQL 参数 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|------|
| `ConfigConstants.LOG_ENDPOINT` (`ENDPOINT`) | `endpoint` | 是 | 无 | 日志服务 Endpoint，例如 `cn-hangzhou.log.aliyuncs.com`。 |
| `ConfigConstants.LOG_ACCESSKEYID` (`ACCESSKEYID`) | `access-key-id` | 是 | 无 | 访问日志服务的 AccessKey ID。 |
| `ConfigConstants.LOG_ACCESSKEY` (`ACCESSKEY`) | `access-key` | 是 | 无 | 访问日志服务的 AccessKey Secret。 |
| `ConfigConstants.LOG_PROJECT` (`PROJECT`) | `project` | 是 | 无 | 要消费的日志服务 Project。 |
| `ConfigConstants.LOG_LOGSTORE` (`LOGSTORE`) | `logstore` | 是 | 无 | 要消费的 Logstore。 |
| `ConfigConstants.LOG_USER_AGENT` (`USER_AGENT`) | 无 | 否 | connector 默认 UA | 自定义访问日志服务时的 User-Agent。 |
| `ConfigConstants.SIGNATURE_VERSION` (`signature.version`) | `signature.version` | 否 | `v1` | 请求签名版本，支持 `v1`、`v4`。 |
| `ConfigConstants.REGION_ID` (`region.id`) | `region.id` | 使用 `v4` 时必填 | 无 | V4 签名使用的地域 ID，例如 `cn-hangzhou`。 |

##### 1.6.2 Source 消费位置和停止位置

| DataStream 参数 | SQL 参数 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|------|
| `ConfigConstants.LOG_CONSUMER_BEGIN_POSITION` (`CONSUMER_BEGIN_POSITION`) | `scan.startup.mode` | 否 | `begin_cursor` / SQL 为 `earliest` | 消费起始位置。支持 `begin_cursor`、`end_cursor`、`consumer_from_checkpoint`、Unix 秒级时间戳；SQL 中对应 `earliest`、`latest`、`checkpoint` 或 Unix 秒级时间戳。 |
| `ConfigConstants.LOG_CONSUMER_DEFAULT_POSITION` (`CONSUMER_DEFAULT_POSITION`) | `scan.startup.default-position` | 否 | `begin_cursor` / SQL 为 `earliest` | 当起始位置为 checkpoint 且服务端没有 checkpoint 时使用的兜底位置。不能设置为 checkpoint。 |
| `ConfigConstants.STOP_TIME` (`stop.time`) | `stop.time` | 否 | 无 | 停止消费的 Unix 秒级时间戳。读取到该时间点后停止对应 shard 的消费，适合离线补数据场景。 |
| `ConfigConstants.LOG_CONSUMERGROUP` (`CONSUMER_GROUP`) | `consumer-group` | 使用 checkpoint 起始位置或需要提交消费进度时必填 | 无 | 日志服务 ConsumerGroup 名称，用于读取或提交服务端 checkpoint。 |

##### 1.6.3 Checkpoint 和消费进度提交

| DataStream 参数 | SQL 参数 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|------|
| `ConfigConstants.LOG_CHECKPOINT_MODE` (`LOG_CHECKPOINT_MODE`) | `checkpoint.mode` | 否 | `ON_CHECKPOINTS` / SQL 为 `on-checkpoints` | 服务端 checkpoint 提交模式。`ON_CHECKPOINTS` 在 Flink checkpoint 完成时提交；`PERIODIC` 独立定时提交；`DISABLED` 不提交到服务端。 |
| `ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS` (`LOG_COMMIT_INTERVAL`) | `commit.interval.ms` | 否 | `10000` | `PERIODIC` 模式下提交服务端 checkpoint 的间隔。 |

`ON_CHECKPOINTS` 需要 Flink 作业开启 checkpointing。无论服务端 checkpoint 是否提交，FLIP-27 Source 的 split/cursor 状态都会参与 Flink checkpoint，用于作业 failover 恢复。

##### 1.6.4 拉取、发现和内存控制

| DataStream 参数 | SQL 参数 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|------|
| `ConfigConstants.LOG_MAX_NUMBER_PER_FETCH` (`MAX_NUMBER_PER_FETCH`) | `max.number.per.fetch` | 否 | `100` | 单次从单个 shard 拉取的最大 LogGroup 数量。调大可提升吞吐，但会增加单次处理和内存压力。 |
| `ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS` (`FETCH_DATA_INTERVAL_MILLIS`) | `fetch.interval.ms` | 否 | `100` | 当一次拉取没有返回数据时，下次拉取前的等待间隔，用于控制空拉频率。 |
| `ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS` (`SHARDS_DISCOVERY_INTERVAL`) | `shards.discovery.interval.ms` | 否 | `60000` | 发现 shard split/merge 的周期。调小可以更快感知 shard 变化，但会增加 `ListShards` 调用频率。 |
| `ConfigConstants.SOURCE_MEMORY_LIMIT` (`source.memory-limit`) | 无 | 否 | `0` | FLIP-27 Source 拉取结果的内存限制，`0` 表示不启用限制。 |
| `ConfigConstants.SOURCE_QUEUE_SIZE` (`source.queue.size`) | 无 | 否 | `1` | 老版 `FlinkLogConsumer` 内部 shard 消费队列大小。 |
| `ConfigConstants.SOURCE_IDLE_INTERVAL` (`source.idle.interval`) | 无 | 否 | `10` | 老版 `FlinkLogConsumer` 在没有活跃 shard 时的空闲等待时间。 |

##### 1.6.5 重试和代理

| DataStream 参数 | SQL 参数 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|------|
| `ConfigConstants.MAX_RETRIES` (`max.retries`) | 无 | 否 | `5` | 普通错误的最大重试次数。 |
| `ConfigConstants.MAX_RETRIES_FOR_RETRYABLE_ERROR` (`max.retries.for.retryable.error`) | 无 | 否 | `60` | 可重试错误的最大重试次数。 |
| `ConfigConstants.BASE_RETRY_BACK_OFF_TIME_MS` (`base.retry.backoff.time.ms`) | 无 | 否 | `200` | 初始重试退避时间。 |
| `ConfigConstants.MAX_RETRY_BACK_OFF_TIME_MS` (`max.retry.backoff.time.ms`) | 无 | 否 | `5000` | 最大重试退避时间。 |
| `ConfigConstants.PROXY_HOST` (`proxy.host`) | 无 | 否 | 无 | HTTP 代理地址。 |
| `ConfigConstants.PROXY_PORT` (`proxy.port`) | 无 | 否 | `-1` | HTTP 代理端口，`-1` 表示不设置。 |
| `ConfigConstants.PROXY_USERNAME` (`proxy.username`) | 无 | 否 | 无 | HTTP 代理用户名。 |
| `ConfigConstants.PROXY_PASSWORD` (`proxy.password`) | 无 | 否 | 无 | HTTP 代理密码。 |
| `ConfigConstants.PROXY_DOMAIN` (`proxy.domain`) | 无 | 否 | 无 | NTLM 代理域。 |
| `ConfigConstants.PROXY_WORKSTATION` (`proxy.workstation`) | 无 | 否 | 无 | NTLM 代理工作站。 |

##### 1.6.6 SQL Connector 专有参数和字段映射

| SQL 参数 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|
| `connector` | 是 | 无 | 固定为 `aliyun-log`。 |
| `ignore-parse-errors` | 否 | `false` | SQL 字段类型转换失败时的处理方式。`false` 表示抛出异常；`true` 表示该字段输出为 `NULL`。 |
| `scan.parallelism` | 否 | Flink 规划决定 | Source 并行度，使用 Flink SQL 标准参数。 |

SQL 普通列按同名 SLS log content 读取并做类型转换。支持的常见类型包括 `STRING`、整数、浮点数、`BOOLEAN`、`DATE`、`TIME`、`TIMESTAMP`。`DATE`、`TIME`、`TIMESTAMP` 可以读取对应格式字符串；`TIMESTAMP` 也可以读取毫秒时间戳字符串。

以下列名为内置元数据列，声明后会从 SLS log 或 shard 元信息中读取，不会从 log content 中取同名字段：

| 元数据列 | 推荐类型 | 含义 |
|------|------|------|
| `__time__` | `TIMESTAMP(3)` | SLS log 时间。秒级时间来自 log time；如果 protobuf 中带有纳秒部分，会合并到毫秒精度。 |
| `__topic__` | `STRING` | LogGroup topic。 |
| `__source__` | `STRING` | LogGroup source。 |
| `__shard__` | `INT` 或 `STRING` | 当前记录所在 shard ID。 |
| `__cursor__` | `STRING` | 当前拉取批次对应的 cursor。 |

#### 1.7 关联 API 与 RAM 权限设置
FlinkLogConsumer 会用到的阿里云日志服务接口如下：
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

### 2. FlinkLogProducer
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
        configProps.put(ConfigConstants.LOG_ACCESSKEYID， sAccessKeyId);
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
* 初始化配置参数 `Properties`。这一步和 Consumer 类似，Producer 也使用 `ConfigConstants` 中的参数。一般情况下使用默认值即可，特殊场景可以按下表调整：

| 参数 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|
| `ConfigConstants.LOG_ENDPOINT` (`ENDPOINT`) | 是 | 无 | 日志服务 Endpoint。 |
| `ConfigConstants.LOG_ACCESSKEYID` (`ACCESSKEYID`) | 是 | 无 | 访问日志服务的 AccessKey ID。 |
| `ConfigConstants.LOG_ACCESSKEY` (`ACCESSKEY`) | 是 | 无 | 访问日志服务的 AccessKey Secret。 |
| `ConfigConstants.LOG_PROJECT` (`PROJECT`) | 是 | 无 | 写入目标 Project。 |
| `ConfigConstants.LOG_LOGSTORE` (`LOGSTORE`) | 是 | 无 | 写入目标 Logstore。 |
| `ConfigConstants.FLUSH_INTERVAL_MS` (`flush.interval.ms`) | 否 | Producer SDK 默认值 | 日志在客户端缓存后等待发送的最长时间，值越小延迟越低，批量压缩效果可能越弱。 |
| `ConfigConstants.IO_THREAD_NUM` (`io.thread.num`) | 否 | Producer SDK 默认值 | 发送日志的 IO 线程数量。 |
| `ConfigConstants.LOG_GROUP_MAX_LINES` (`logGroup.max.lines`) | 否 | Producer SDK 默认值 | 单个 LogGroup 最多包含的日志条数。 |
| `ConfigConstants.LOG_GROUP_MAX_SIZE` (`logGroup.max.size`) | 否 | Producer SDK 默认值 | 单个 LogGroup 的最大字节数。 |
| `ConfigConstants.TOTAL_SIZE_IN_BYTES` (`total.size.in.bytes`) | 否 | Producer SDK 默认值 | Producer 客户端可使用的总缓存大小。 |
| `ConfigConstants.MAX_RETRIES` (`max.retries`) | 否 | Producer SDK 默认值 | 普通发送失败的重试次数。 |
| `ConfigConstants.BASE_RETRY_BACK_OFF_TIME_MS` (`base.retry.backoff.time.ms`) | 否 | Producer SDK 默认值 | 发送失败后的初始重试退避时间。 |
| `ConfigConstants.MAX_RETRY_BACK_OFF_TIME_MS` (`max.retry.backoff.time.ms`) | 否 | Producer SDK 默认值 | 发送失败后的最大重试退避时间。 |
| `ConfigConstants.MAX_BLOCK_TIME_MS` (`max.block.time.ms`) | 否 | Producer SDK 默认值 | 缓存满或资源不足时，发送调用最多阻塞等待的时间。 |
| `ConfigConstants.BUCKETS` (`producer.buckets`) | 否 | Producer SDK 默认值 | Producer 内部分桶数量，用于并发和批量聚合。 |
| `ConfigConstants.PRODUCER_ADJUST_SHARD_HASH` (`producer.adjust.shard.hash`) | 否 | `true` | 是否自动调整 shard hash。 |
| `ConfigConstants.SIGNATURE_VERSION` (`signature.version`) | 否 | `v1` | 请求签名版本，支持 `v1`、`v4`。 |
| `ConfigConstants.REGION_ID` (`region.id`) | 使用 `v4` 时必填 | 无 | V4 签名使用的地域 ID。 |

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

当RAM子用户使用Producer时，需要对上述两个API进行授权：

|接口|资源|
|------|------|
|log:PostLogStoreLogs| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|
