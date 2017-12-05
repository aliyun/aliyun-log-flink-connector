# Flink log connector
## 介绍
Flink log connector是阿里云日志服务提供的,用于对接flink的工具,包括两部分,消费者(Consumer)和生产者(Producer).

消费者用于从日志服务中读取数据,支持exactly once语义,支持shard负载均衡.
生产者用于将数据写入日志服务,使用connector时,需要在项目中添加maven依赖:
```
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-log-connector</artifactId>
  <version>0.1.0</version>
</dependency>
```
## 用法
使用前,请参考[日志服务文档](https://help.aliyun.com/document_detail/54604.html),正确创建日志服务资源.
如果使用子账号访问,请确认正确设置了logstore的RAM策略,参考文档[授权RAM子用户访问日志服务资源](https://help.aliyun.com/document_detail/47664.html).
### Log Consumer
在Connector中, 类FlinkLogConsumer提供了订阅日志服务中某一个logStore的能力,实现了exactly语义,在使用时,用户无需关心logStore中shard数
量的变化,consumer会自动感知.flink中每一个子任务负责消费logStore中部分shard,如果logStore中shard发生split或者merge,子任务消费的shard也会随之改变.

```
Properties configProps = new Properties();
// 设置访问日志服务的域名
configProps.put(ConfigConstants.LOG_ENDPOINT, "cn-hangzhou.log.aliyuncs.com");
// 设置访问ak
configProps.put(ConfigConstants.LOG_ACCESSSKEYID, "");
configProps.put(ConfigConstants.LOG_ACCESSKEY, "");
// 设置日志服务的project
configProps.put(ConfigConstants.LOG_PROJECT, "ali-cn-hangzhou-sls-admin");
// 设置日志服务的logStore
configProps.put(ConfigConstants.LOG_LOGSTORE, "sls_consumergroup_log");
// 设置消费日志服务起始位置
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_END_CURSOR);
// 设置日志服务的消息反序列化方法
RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<RawLogGroupList> logTestStream = env.addSource(
        new FlinkLogConsumer<RawLogGroupList>(deserializer, configProps));
```
上面是一个简单的消费示例,我们使用java.util.Properties作为配置工具,所有Consumer的配置都可以在ConfigConstants中找到.
注意,flink stream的子任务数量和日志服务logStore中的shard数量是独立的,如果shard数量多于子任务数量,每个子任务不重复的消费多个shard,如果少于,
那么部分子任务就会空闲,等到新的shard产生.
#### 设置消费起始位置
Flink log consumer支持设置shard的消费起始位置,通过设置属性ConfigConstants.LOG_CONSUMER_BEGIN_POSITION,就可以定制消费从shard的头尾或者某个特定时间开始消费,具体取值如下:

* Consts.LOG_BEGIN_CURSOR: 表示从shard的头开始消费,也就是从shard中最旧的数据开始消费.
* Consts.LOG_END_CURSOR: 表示从shard的尾开始,也就是从shard中最新的数据开始消费.
* UnixTimestamp: 一个整型数值的字符串,用1970-01-01到现在的秒数表示, 含义是消费shard中这个时间点之后的数据.

三种取值举例如下:
```
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_BEGIN_CURSOR);
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_END_CURSOR);
configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, "1512439000");
```

#### 设置消费进度监控
Flink log consumer支持可选的设置消费进度监控,所谓消费进度就是获取每一个shard实时的消费位置,这个位置使用时间戳表示,详细概念可以参考
文档[消费组-查看状态](https://help.aliyun.com/document_detail/43998.html),[消费组-监控报警
](https://help.aliyun.com/document_detail/55912.html).
```
configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "you consumer group name");
```
通过上面的代码就可以设置消费进度监控,注意上面代码是可选的,如果设置了,consumer会首先创建consumerGroup,如果已经存在,则什么都不错,consumer中的snapshot会自动同步到日志服务的consumerGroup中,用户可以在日志服务的控制台查看consumer的消费进度.
#### 容灾和exactly once语义支持
当打开Flink的checkpointing功能时,Flink log consumer会周期性的将每个shard的消费进度保存起来,当作业失败时,flink会从恢复log consumer,并
从保存的最新的checkpoint开始消费.

写checkpoint的周期定义了当发生失败时,最多多少的数据会被回溯,也就是重新消费,使用代码如下:
```
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 开启flink exactly once语义
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 每5s保存一次checkpoint
env.enableCheckpointing(5000);
```
更多Flink checkpoint的细节请参考Flink官方文档[Checkpoints](https://ci.apache.org/projects/flink/flink-docs-release-1.3/setup/checkpoints.html).
#### 关联的日志服务 API
Flink log consumer 会用到的阿里云日志服务接口如下:
* GetCursorOrData

    用于从shard中拉数据, 注意频繁的调用该接口可能会导致数据超过日志服务的shard quota, 可以通过ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS和ConfigConstants.LOG_MAX_NUMBER_PER_FETCH
    控制接口调用的时间间隔和每次调用拉取的日志数量,shard的quota参考文章[shard简介](https://help.aliyun.com/document_detail/28976.html).
    ```
    configProps.put(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, "100");
    configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "100");
    ```
* ListShards

     用于获取logStore中所有的shard列表,获取shard状态等.如果您的shard经常发生分裂合并,可以通过调整接口的调用周期来及时发现shard的变化.
     ```
     // 设置每30s调用一次ListShards
     configProps.put(ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS, "30000");
     ```
* CreateConsumerGroup
    该接口调用只有当设置消费进度监控时才会发生,功能是创建consumerGroup,用于同步checkpoint.

子用户使用Flink log consumer需要授权如下几个RAM Policy:

|接口|资源|
|------|------|
|log:GetCursorOrData| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|
|log:ListShards| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}|
|log:CreateConsumerGroup| acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName}/consumergroup/*|

### Log Producer
