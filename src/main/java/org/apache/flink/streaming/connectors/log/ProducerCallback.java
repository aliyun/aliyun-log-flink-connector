package org.apache.flink.streaming.connectors.log;

import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.TagContent;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.producer.ILogCallback;
import com.aliyun.openservices.log.producer.LogProducer;
import com.aliyun.openservices.log.response.PutLogsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProducerCallback extends ILogCallback {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerCallback.class);
    private String logProject;
    private String logStore;
    private String logTopic = "";
    private String logSource;
    private String shardHashKey;
    private List<LogItem> logs;
    private LogProducer logProducer;

    public void onCompletion(PutLogsResponse response, LogException e) {
        if(e == null){
            if(LOG.isDebugEnabled()){
                LOG.debug("post logs success, requestid: {}", response.GetRequestId());
            }
        }
        else{
            LOG.info("fail to post logs, errorcode: {}, errormessage: {}, requestid: {}", e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
        }
    }
    public ProducerCallback clone(){
        return new ProducerCallback();
    }

    public void init(LogProducer producer, String project, String logStore, String topic, String shardHash, String source, List<LogItem> logItems){
        this.logProducer = producer;
        this.logProject = project;
        this.logStore = logStore;
        this.logTopic = topic;
        this.shardHashKey = shardHash;
        this.logSource = source;
        this.logs = logItems;
    }

    public String getLogProject() {
        return logProject;
    }

    public void setLogProject(String logProject) {
        this.logProject = logProject;
    }

    public String getLogStore() {
        return logStore;
    }

    public void setLogStore(String logStore) {
        this.logStore = logStore;
    }

    public String getLogTopic() {
        return logTopic;
    }

    public void setLogTopic(String logTopic) {
        this.logTopic = logTopic;
    }

    public String getLogSource() {
        return logSource;
    }

    public void setLogSource(String logSource) {
        this.logSource = logSource;
    }

    public List<LogItem> getLogs() {
        return logs;
    }

    public void setLogs(List<LogItem> logs) {
        this.logs = logs;
    }

    public String getShardHashKey() {
        return shardHashKey;
    }

    public void setShardHashKey(String shardHashKey) {
        this.shardHashKey = shardHashKey;
    }

    public LogProducer getLogProducer() {
        return logProducer;
    }

    public void setLogProducer(LogProducer logProducer) {
        this.logProducer = logProducer;
    }
}
