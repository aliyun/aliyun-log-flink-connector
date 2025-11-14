package com.aliyun.openservices.log.flink.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

/**
 * Serializer for AliyunLogSourceSplit.
 */
public class AliyunLogSourceSplitSerializer implements SimpleVersionedSerializer<AliyunLogSourceSplit> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(AliyunLogSourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(split);
            return baos.toByteArray();
        }
    }

    @Override
    public AliyunLogSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (AliyunLogSourceSplit) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize split", e);
        }
    }
}


