# 新版 Source / Sink / SQL Connector 使用说明

本文档只说明新版接口：FLIP-27 `AliyunLogSource`、FLIP-27 `AliyunLogSink` 和 SQL Connector。旧 `FlinkLogConsumer` / `FlinkLogProducer` 属于老 DataStream 接口，后续会删除，不要和本文档中的参数混用。

除特别说明外，时间单位均为毫秒，Unix 时间戳单位为秒。

## Maven 依赖

```xml
<dependency>
    <groupId>com.aliyun.openservices</groupId>
    <artifactId>flink-log-connector</artifactId>
    <version>0.1.45</version>
</dependency>
<dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>2.5.0</version>
</dependency>
```

## DataStream Source

`AliyunLogSource` 使用 Flink FLIP-27 Source API，通过 `env.fromSource(...)` 接入。Source 的 split/cursor 状态会参与 Flink checkpoint，用于作业 failover 恢复；如果配置了 ConsumerGroup，还可以按配置将 checkpoint 提交到日志服务服务端。

```java
Properties properties = new Properties();
properties.setProperty(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.ON_CHECKPOINTS.name());
properties.setProperty(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "100");

AliyunLogSource<MyRecord> source = AliyunLogSource.<MyRecord>builder()
        .setProject("your-project")
        .setLogStore("your-logstore")
        .setEndpoint("cn-hangzhou.log.aliyuncs.com")
        .setCredentials(accessKeyId, accessKeySecret)
        .setConsumerGroup("flink-source-consumer")
        .setStartingPosition(StartingPosition.EARLIEST)
        .setProperties(properties)
        .setDeserializer(new MyDeserializer())
        .build();

DataStream<MyRecord> stream = env.fromSource(
        source,
        WatermarkStrategy.noWatermarks(),
        "aliyun-log-source");
```

### Source 参数

| 参数 / Builder 方法 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|
| `setProject(String project)` | 是 | 无 | 要消费的日志服务 Project。 |
| `setLogStore(String logstore)` | 是 | 无 | 要消费的 Logstore。 |
| `setEndpoint(String endpoint)` | 是 | 无 | 日志服务 Endpoint，例如 `cn-hangzhou.log.aliyuncs.com`。 |
| `setCredentials(String accessKeyId, String accessKey)` | 是 | 无 | 访问日志服务的 AccessKey ID 和 AccessKey Secret。 |
| `setDeserializer(AliyunLogDeserializationSchema<T> deserializer)` | 是 | 无 | 将 SLS 拉取结果转换成 Flink 记录的反序列化器。 |
| `setConsumerGroup(String consumerGroup)` | 否 | 无 | 日志服务 ConsumerGroup 名称，用于读取或提交服务端 checkpoint。使用 checkpoint 起始位置或需要服务端消费进度时应设置。 |
| `setStartingPosition(StartingPosition)` / `setStartingPosition(String)` | 否 | `earliest` | 消费起始位置。支持 `earliest`、`latest`、`checkpoint` 或 Unix 秒级时间戳；兼容 `begin_cursor`、`end_cursor`、`consumer_from_checkpoint`。 |
| `setFallbackPosition(StartingPosition)` | 否 | `earliest` | 起始位置为 checkpoint 且服务端没有 checkpoint 时使用的兜底位置，不能设置为 checkpoint。 |
| `setSplitAssigner(AliyunLogSplitAssigner)` | 否 | `ModuloSplitAssigner` | shard 到 source reader 的分配策略。默认按 shard id 和并行度取模，也可以使用 `RoundRobinSplitAssigner` 或自定义实现。 |
| `setProperty(String key, String value)` / `setProperties(Properties properties)` | 否 | 无 | 设置高级参数。 |
| `ConfigConstants.STOP_TIME` (`stop.time`) | 否 | 无 | 停止消费的 Unix 秒级时间戳。读取到该时间点后停止对应 shard 的消费，适合离线补数据。 |
| `ConfigConstants.LOG_CHECKPOINT_MODE` (`LOG_CHECKPOINT_MODE`) | 否 | `ON_CHECKPOINTS` | 服务端 checkpoint 提交模式。`ON_CHECKPOINTS` 在 Flink checkpoint 完成时提交；`PERIODIC` 独立定时提交；`DISABLED` 不提交到服务端。 |
| `ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS` (`LOG_COMMIT_INTERVAL`) | 否 | `10000` | `PERIODIC` 模式下提交服务端 checkpoint 的间隔。 |
| `ConfigConstants.LOG_MAX_NUMBER_PER_FETCH` (`MAX_NUMBER_PER_FETCH`) | 否 | `100` | 单次从单个 shard 拉取的最大 LogGroup 数量。调大可提升吞吐，但会增加单次处理和内存压力。 |
| `ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS` (`FETCH_DATA_INTERVAL_MILLIS`) | 否 | `100` | 当一次拉取没有返回数据时，下次拉取前的等待间隔，用于控制空拉频率。 |
| `ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS` (`SHARDS_DISCOVERY_INTERVAL`) | 否 | `60000` | 发现 shard split/merge 的周期。调小可以更快感知 shard 变化，但会增加 `ListShards` 调用频率。 |
| `ConfigConstants.SOURCE_MEMORY_LIMIT` (`source.memory-limit`) | 否 | `0` | 拉取结果的内存限制，`0` 表示不启用限制。 |
| `ConfigConstants.LOG_USER_AGENT` (`USER_AGENT`) | 否 | connector 默认 UA | 自定义访问日志服务时的 User-Agent。 |
| `ConfigConstants.SIGNATURE_VERSION` (`signature.version`) | 否 | `v1` | 请求签名版本，支持 `v1`、`v4`。 |
| `ConfigConstants.REGION_ID` (`region.id`) | 使用 `v4` 时必填 | 无 | V4 签名使用的地域 ID，例如 `cn-hangzhou`。 |
| `ConfigConstants.MAX_RETRIES` (`max.retries`) | 否 | `5` | 普通错误的最大重试次数。 |
| `ConfigConstants.MAX_RETRIES_FOR_RETRYABLE_ERROR` (`max.retries.for.retryable.error`) | 否 | `60` | 可重试错误的最大重试次数。 |
| `ConfigConstants.BASE_RETRY_BACK_OFF_TIME_MS` (`base.retry.backoff.time.ms`) | 否 | `200` | 初始重试退避时间。 |
| `ConfigConstants.MAX_RETRY_BACK_OFF_TIME_MS` (`max.retry.backoff.time.ms`) | 否 | `5000` | 最大重试退避时间。 |
| `ConfigConstants.PROXY_HOST` (`proxy.host`) | 否 | 无 | HTTP 代理地址。 |
| `ConfigConstants.PROXY_PORT` (`proxy.port`) | 否 | `-1` | HTTP 代理端口，`-1` 表示不设置。 |
| `ConfigConstants.PROXY_USERNAME` (`proxy.username`) | 否 | 无 | HTTP 代理用户名。 |
| `ConfigConstants.PROXY_PASSWORD` (`proxy.password`) | 否 | 无 | HTTP 代理密码。 |
| `ConfigConstants.PROXY_DOMAIN` (`proxy.domain`) | 否 | 无 | NTLM 代理域。 |
| `ConfigConstants.PROXY_WORKSTATION` (`proxy.workstation`) | 否 | 无 | NTLM 代理工作站。 |

## DataStream Sink

`AliyunLogSink` 使用 Flink Sink V2 API，通过 `stream.sinkTo(...)` 接入。Sink 基于日志服务 Producer SDK 异步发送数据，并在 Flink checkpoint 或作业结束时等待已提交请求完成，提供 at-least-once 语义。日志服务 Producer 不提供可与 Flink 协调的事务协议，因此不声明 exactly-once。

自定义序列化使用 `AliyunLogSerializationSchema<T>`。一个输入元素可以通过 `Collector<SinkRecord>` 输出零条、一条或多条 SLS 记录。新版 Sink 不兼容旧 `LogSerializationSchema` / `LogSerializationSchemaV2`。

```java
class MySerializationSchema implements AliyunLogSerializationSchema<String> {
    @Override
    public void serialize(String element, Collector<SinkRecord> output) {
        LogItem item = new LogItem((int) (System.currentTimeMillis() / 1000L));
        item.PushBack("message", element);

        SinkRecord record = new SinkRecord();
        record.setTopic("flink");
        record.setSource("flink-job");
        record.setLogItem(item);
        output.collect(record);
    }
}

AliyunLogSink<String> sink = AliyunLogSink.<String>builder()
        .setProject("your-project")
        .setLogStore("your-logstore")
        .setEndpoint("cn-hangzhou.log.aliyuncs.com")
        .setCredentials(accessKeyId, accessKeySecret)
        .setSerializer(new MySerializationSchema())
        .setProperty(ConfigConstants.FLUSH_INTERVAL_MS, "100")
        .build();

stream.sinkTo(sink).name("aliyun-log-sink");
```

### Sink 参数

| 参数 / Builder 方法 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|
| `setProject(String project)` | 是 | 无 | 写入目标 Project。 |
| `setLogStore(String logstore)` | 是 | 无 | 默认写入目标 Logstore。单条 `SinkRecord` 设置了 logstore 时会覆盖该默认值。 |
| `setEndpoint(String endpoint)` | 是 | 无 | 日志服务 Endpoint。 |
| `setCredentials(String accessKeyId, String accessKey)` | 是 | 无 | 访问日志服务的 AccessKey ID 和 AccessKey Secret。 |
| `setSerializer(AliyunLogSerializationSchema<T> serializer)` | 是 | 无 | 将 Flink 记录转换为 `SinkRecord` 的序列化器。 |
| `setProperty(String key, String value)` / `setProperties(Properties properties)` | 否 | 无 | 设置 Producer 高级参数。 |
| `ConfigConstants.FLUSH_INTERVAL_MS` (`flush.interval.ms`) | 否 | Producer SDK 默认值 | 日志在客户端缓存后等待发送的最长时间。 |
| `ConfigConstants.MAX_RETRIES` (`max.retries`) | 否 | Producer SDK 默认值 | 普通发送失败的最大重试次数。 |
| `ConfigConstants.BASE_RETRY_BACK_OFF_TIME_MS` (`base.retry.backoff.time.ms`) | 否 | Producer SDK 默认值 | 发送失败后的初始重试退避时间。 |
| `ConfigConstants.MAX_RETRY_BACK_OFF_TIME_MS` (`max.retry.backoff.time.ms`) | 否 | Producer SDK 默认值 | 发送失败后的最大重试退避时间。 |
| `ConfigConstants.MAX_BLOCK_TIME_MS` (`max.block.time.ms`) | 否 | Producer SDK 默认值 | 缓存满或资源不足时，发送调用最多阻塞等待的时间。 |
| `ConfigConstants.IO_THREAD_NUM` (`io.thread.num`) | 否 | Producer SDK 默认值 | 发送日志的 IO 线程数量。 |
| `ConfigConstants.BUCKETS` (`producer.buckets`) | 否 | Producer SDK 默认值 | Producer 内部分桶数量，用于并发和批量聚合。 |
| `ConfigConstants.TOTAL_SIZE_IN_BYTES` (`total.size.in.bytes`) | 否 | Producer SDK 默认值 | Producer 客户端可使用的总缓存大小。 |
| `ConfigConstants.PRODUCER_ADJUST_SHARD_HASH` (`producer.adjust.shard.hash`) | 否 | `true` | 是否由 Producer 自动调整 shard hash。 |
| `ConfigConstants.SIGNATURE_VERSION` (`signature.version`) | 否 | `v1` | 请求签名版本，支持 `v1`、`v4`。 |
| `ConfigConstants.REGION_ID` (`region.id`) | 使用 `v4` 时必填 | 无 | V4 签名使用的地域 ID。 |

## SQL Connector

SQL Connector 的标识为 `aliyun-log`，同一个 connector 同时支持 SQL Source 和 SQL Sink。Source 普通列按同名 SLS log content 读取；Sink 普通列按列名写入 SLS log content。

### SQL Source 示例

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

### SQL Sink 示例

```sql
CREATE TABLE sls_sink (
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
  'sink.topic' = 'flink-sql',
  'sink.source' = 'flink-job',
  'flush.interval.ms' = '100',
  'max.retries' = '5'
);
```

### SQL WITH 参数

| SQL 参数 | 适用方向 | 是否必填 | 默认值 | 含义 |
|------|------|------|------|------|
| `connector` | Source / Sink | 是 | 无 | 固定为 `aliyun-log`。 |
| `endpoint` | Source / Sink | 是 | 无 | 日志服务 Endpoint，例如 `cn-hangzhou.log.aliyuncs.com`。 |
| `project` | Source / Sink | 是 | 无 | 日志服务 Project。 |
| `logstore` | Source / Sink | 是 | 无 | Source 读取或 Sink 默认写入的 Logstore。 |
| `access-key-id` | Source / Sink | 是 | 无 | 访问日志服务的 AccessKey ID。 |
| `access-key` | Source / Sink | 是 | 无 | 访问日志服务的 AccessKey Secret。 |
| `consumer-group` | Source | 否 | 无 | ConsumerGroup 名称，用于读取或提交服务端 checkpoint。 |
| `scan.startup.mode` | Source | 否 | `earliest` | 消费起始位置。支持 `earliest`、`latest`、`checkpoint` 或 Unix 秒级时间戳。 |
| `scan.startup.default-position` | Source | 否 | `earliest` | 起始位置为 checkpoint 且服务端没有 checkpoint 时使用的兜底位置，不能设置为 checkpoint。 |
| `checkpoint.mode` | Source | 否 | `on-checkpoints` | 服务端 checkpoint 提交模式。支持 `on-checkpoints`、`periodic`、`disabled`。 |
| `commit.interval.ms` | Source | 否 | `10000` | `periodic` 模式下提交服务端 checkpoint 的间隔。 |
| `fetch.interval.ms` | Source | 否 | `100` | 一次拉取没有返回数据时，下次拉取前的等待间隔。 |
| `max.number.per.fetch` | Source | 否 | `100` | 单次从单个 shard 拉取的最大 LogGroup 数量。 |
| `shards.discovery.interval.ms` | Source | 否 | `60000` | 发现 shard split/merge 的周期。 |
| `stop.time` | Source | 否 | 无 | 停止消费的 Unix 秒级时间戳。 |
| `ignore-parse-errors` | Source | 否 | `false` | 字段类型转换失败时是否输出 `NULL`。`false` 表示抛出异常。 |
| `scan.parallelism` | Source | 否 | Flink 规划决定 | Source 并行度。 |
| `sink.topic` | Sink | 否 | `""` | SQL Sink 写入时默认使用的 LogGroup topic，可被 `__topic__` 列覆盖。 |
| `sink.source` | Sink | 否 | 无 | SQL Sink 写入时默认使用的 LogGroup source，可被 `__source__` 列覆盖。 |
| `sink.parallelism` | Sink | 否 | Flink 规划决定 | Sink 并行度。 |
| `flush.interval.ms` | Sink | 否 | Producer SDK 默认值 | 日志在客户端缓存后等待发送的最长时间，值越小延迟越低，批量压缩效果可能越弱。 |
| `max.retries` | Sink | 否 | Producer SDK 默认值 | Producer 普通发送失败的最大重试次数。 |
| `base.retry.backoff.time.ms` | Sink | 否 | Producer SDK 默认值 | Producer 发送失败后的初始重试退避时间。 |
| `max.retry.backoff.time.ms` | Sink | 否 | Producer SDK 默认值 | Producer 发送失败后的最大重试退避时间。 |
| `max.block.time.ms` | Sink | 否 | Producer SDK 默认值 | Producer 缓存满或资源不足时，发送调用最多阻塞等待的时间。 |
| `io.thread.num` | Sink | 否 | Producer SDK 默认值 | Producer 发送日志的 IO 线程数量。 |
| `producer.buckets` | Sink | 否 | Producer SDK 默认值 | Producer 内部分桶数量，用于并发和批量聚合。 |
| `total.size.in.bytes` | Sink | 否 | Producer SDK 默认值 | Producer 客户端可使用的总缓存大小。 |
| `producer.adjust.shard.hash` | Sink | 否 | `true` | 是否由 Producer 自动调整 shard hash。 |
| `signature.version` | Source / Sink | 否 | `v1` | 请求签名版本，支持 `v1`、`v4`。 |
| `region.id` | Source / Sink | 使用 `v4` 时必填 | 无 | V4 签名使用的地域 ID。 |

### SQL Source 元数据列

以下列名为内置读取元数据列，声明后会从 SLS log 或 shard 元信息中读取，不会从 log content 中取同名字段：

| 元数据列 | 推荐类型 | 含义 |
|------|------|------|
| `__time__` | `TIMESTAMP(3)` | SLS log 时间。秒级时间来自 log time；如果 protobuf 中带有纳秒部分，会合并到毫秒精度。 |
| `__topic__` | `STRING` | LogGroup topic。 |
| `__source__` | `STRING` | LogGroup source。 |
| `__shard__` | `INT` 或 `STRING` | 当前记录所在 shard ID。 |
| `__cursor__` | `STRING` | 当前拉取批次对应的 cursor。 |

### SQL Sink 元数据列

以下列名为内置写入元数据列，声明后不会作为普通 content 写入：

| 元数据列 | 推荐类型 | 含义 |
|------|------|------|
| `__time__` | `TIMESTAMP(3)`、整数或字符串 | 写入 SLS log time。时间戳类型会按秒写入；整数和字符串按 Unix 秒级时间戳解析。 |
| `__topic__` | `STRING` | 覆盖 `sink.topic`，设置当前记录的 topic。 |
| `__source__` | `STRING` | 覆盖 `sink.source`，设置当前记录的 source。 |
| `__logstore__` | `STRING` | 覆盖表参数 `logstore`，将当前记录写入指定 Logstore。 |
| `__hash_key__` | `STRING` | 设置当前记录的 shard hash key。 |

## RAM 权限

Source 读取需要授权以下日志服务 API：

| API | Resource |
|------|------|
| `log:GetCursorOrData` | `acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}` |
| `log:ListShards` | `acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}` |
| `log:CreateConsumerGroup` | `acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}/consumergroup/*` |
| `log:ConsumerGroupUpdateCheckPoint` | `acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}/consumergroup/${consumerGroupName}` |

Sink 写入需要授权以下日志服务 API：

| API | Resource |
|------|------|
| `log:PostLogStoreLogs` | `acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}` |
