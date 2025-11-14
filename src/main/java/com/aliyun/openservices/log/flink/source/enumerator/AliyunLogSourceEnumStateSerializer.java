package com.aliyun.openservices.log.flink.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

/**
 * Serializer for AliyunLogSourceEnumState.
 */
public class AliyunLogSourceEnumStateSerializer implements SimpleVersionedSerializer<AliyunLogSourceEnumState> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(AliyunLogSourceEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(state);
            return baos.toByteArray();
        }
    }

    @Override
    public AliyunLogSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (AliyunLogSourceEnumState) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize enumerator state", e);
        }
    }
}


