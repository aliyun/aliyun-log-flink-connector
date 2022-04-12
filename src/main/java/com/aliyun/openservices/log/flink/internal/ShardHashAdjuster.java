package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.Consts;
import org.apache.flink.util.Preconditions;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ShardHashAdjuster {

    private static final int HEX_LENGTH = 32;
    private static final int BINARY_LENGTH = 128;

    private final int reservedBits;

    public ShardHashAdjuster(int bucket) {
        reservedBits = Integer.bitCount(bucket - 1);
    }

    public static String padStart(String string, int minLength, char padChar) {
        Preconditions.checkNotNull(string);
        if (string.length() >= minLength) {
            return string;
        }
        StringBuilder sb = new StringBuilder(minLength);
        for (int i = string.length(); i < minLength; i++) {
            sb.append(padChar);
        }
        sb.append(string);
        return sb.toString();
    }

    public static String padEnd(String string, int minLength, char padChar) {
        Preconditions.checkNotNull(string);
        if (string.length() >= minLength) {
            return string;
        }
        StringBuilder sb = new StringBuilder(minLength);
        sb.append(string);
        for (int i = string.length(); i < minLength; i++) {
            sb.append(padChar);
        }
        return sb.toString();
    }

    public String adjust(String shardHash) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(Consts.CONST_MD5);
            String md5Str = new BigInteger(1, messageDigest.digest(shardHash.getBytes())).toString(2);
            String binary = padStart(md5Str, BINARY_LENGTH, '0');
            String adjustedBinary = padEnd(binary.substring(0, reservedBits), BINARY_LENGTH, '0');
            return padStart(new BigInteger(adjustedBinary, 2).toString(16), HEX_LENGTH, '0');
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }
}
