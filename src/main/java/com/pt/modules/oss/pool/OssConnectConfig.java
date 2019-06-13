package com.pt.modules.oss.pool;


import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 *  oss链接配置项
 */
public class OssConnectConfig extends GenericObjectPoolConfig {

    private static String endpoint;
    private static String accessKeyId;
    private static String accessKeySecret;
    private static String bucketName;
    private static String downloadFile;//本地路径
    private static int taskNum; //最大下载数
    private static long partSize;//分片大小
    private static boolean enableCheckpoint;//开启断点续传

    public static String getEndpoint() {
        return endpoint;
    }

    public static void setEndpoint(String endpoint) {
        OssConnectConfig.endpoint = endpoint;
    }

    public static String getAccessKeyId() {
        return accessKeyId;
    }

    public static void setAccessKeyId(String accessKeyId) {
        OssConnectConfig.accessKeyId = accessKeyId;
    }

    public static String getAccessKeySecret() {
        return accessKeySecret;
    }

    public static void setAccessKeySecret(String accessKeySecret) {
        OssConnectConfig.accessKeySecret = accessKeySecret;
    }

    public static String getBucketName() {
        return bucketName;
    }

    public static void setBucketName(String bucketName) {
        OssConnectConfig.bucketName = bucketName;
    }

    public static String getDownloadFile() {
        return downloadFile;
    }

    public static void setDownloadFile(String downloadFile) {
        OssConnectConfig.downloadFile = downloadFile;
    }

    public static int getTaskNum() {
        return taskNum;
    }

    public static void setTaskNum(int taskNum) {
        OssConnectConfig.taskNum = taskNum;
    }

    public static long getPartSize() {
        return partSize;
    }

    public static void setPartSize(long partSize) {
        OssConnectConfig.partSize = partSize;
    }

    public static boolean getEnableCheckpoint() {
        return enableCheckpoint;
    }

    public static void setEnableCheckpoint(boolean enableCheckpoint) {
        OssConnectConfig.enableCheckpoint = enableCheckpoint;
    }
}
