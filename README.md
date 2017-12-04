# flink log connector
## 介绍
flink log connector是阿里云日志服务提供的,用于对接flink的工具,包括两部分,消费者(Consumer)和生产者(Producer).

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
