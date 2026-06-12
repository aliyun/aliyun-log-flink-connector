# Aliyun Log Flink Connector

阿里云日志服务 Flink Connector，支持从日志服务读取数据以及向日志服务写入数据。

## 推荐文档

| 文档 | 适用场景 |
|------|------|
| [新版 Source / Sink / SQL Connector 使用说明](./FLIP27_SOURCE_SINK_CN.md) | 新作业推荐使用。包含 FLIP-27 `AliyunLogSource`、FLIP-27 `AliyunLogSink` 和 SQL Connector。 |
| [中文使用说明](./README_CN.md) | 总览和旧 DataStream 接口说明。旧 `FlinkLogConsumer` / `FlinkLogProducer` 后续会删除。 |
| [English User Guide](./README_EN.md) | Legacy English guide. |

## Maven

```xml
<dependency>
    <groupId>com.aliyun.openservices</groupId>
    <artifactId>flink-log-connector</artifactId>
    <version>0.1.45</version>
</dependency>
```

## 接口选择

| 接口 | 状态 | 说明 |
|------|------|------|
| `AliyunLogSource` | 推荐 | 基于 Flink FLIP-27 Source API。 |
| `AliyunLogSink` | 推荐 | 基于 Flink Sink V2 API，提供 at-least-once 写入语义。 |
| SQL Connector `aliyun-log` | 推荐 | 同时支持 SQL Source 和 SQL Sink。 |
| `FlinkLogConsumer` | 旧接口 | 老 DataStream Source，后续会删除。 |
| `FlinkLogProducer` / `FlinkLogProducerV2` | 旧接口 | 老 DataStream Sink，后续会删除。 |
