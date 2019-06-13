package com.pt.modules.oss;


import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.*;
import com.pt.modules.oss.pool.OssConnectConfig;
import com.pt.modules.oss.pool.OssConnectObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.UUID;

/**
 * 阿里巴巴云服务oss 操作工具类
 */
public class OssUtil extends OssConnectConfig {


    static Logger log = LoggerFactory.getLogger(OssUtil.class);
    private OssConnectObjectPool pool = new OssConnectObjectPool();
    /**
     * 打开服务 保持打开状态不用关闭
     *
     * @return
     */
//    public OssUtil() {
//        if(ossClient==null){
//            ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret);
//            log.info("创建oss链接");
//        }
////        return ossClient;
//    }

    /**
     * 上传文件
     *
     * @param uploadFile    本地路径
     * @return 远程路径
     */
    public String uplFile(String uploadFile) {

        String ext = uploadFile.substring(uploadFile.lastIndexOf("."), uploadFile.length());
        String filename = getId() + ext;
        String remotePath = getEndpoint()+"/"+getBucketName()+"/"+filename;
        OSSClient ossClient=null;
        try {
            ossClient = pool.borrowObject();
            log.info("远程路径为："+remotePath);
            UploadFileRequest uploadFileRequest = new UploadFileRequest(getBucketName(), filename);
            // 待上传的本地文件
            uploadFileRequest.setUploadFile(uploadFile);
            // 设置并发下载数，默认1
            uploadFileRequest.setTaskNum(5);
            // 设置分片大小，默认100KB
            uploadFileRequest.setPartSize(1024 * 1024 * 1);
            // 开启断点续传，默认关闭
            uploadFileRequest.setEnableCheckpoint(true);
//            ObjectMetadata objectMetadata = new ObjectMetadata();
//            objectMetadata.setObjectAcl(CannedAccessControlList.PublicRead);
//            uploadFileRequest.setObjectMetadata(objectMetadata);
            UploadFileResult uploadResult = ossClient.uploadFile(uploadFileRequest);
            //设置文件权限，公有读
            ossClient.setObjectAcl(getBucketName(), filename, CannedAccessControlList.PublicRead);

            CompleteMultipartUploadResult multipartUploadResult =
                    uploadResult.getMultipartUploadResult();
            log.info("上传成功");
        } catch (Exception ce) {
            log.info("上传失败");
            log.info("Error Message: " + ce.getMessage());
        } catch (Throwable e) {
            log.info("上传失败");
            log.info("Throwable Message: " + e.getMessage());
        } finally {
            pool.returnObject(ossClient);
            //ossClient.shutdown();
        }
        return remotePath;
    }

    /**
     * 上传流文件
     * @param bytes
     * @return
     */
    public String uplFileByBytes(byte[] bytes) {

//        String ext = uploadFile.substring(uploadFile.lastIndexOf("."), uploadFile.length());
        String filename = getEndpoint()+"/"+getBucketName()+"/"+getId() + ".pdf";
//        String filename = getId() + ".pdf";
        OSSClient ossClient=null;
        try {
            ossClient = pool.borrowObject();

            ossClient.putObject(getBucketName(), filename, new ByteArrayInputStream(bytes));
            //设置文件权限，公有读
            ossClient.setObjectAcl(getBucketName(), filename, CannedAccessControlList.PublicRead);

            log.info("上传成功");
        } catch (Exception ce) {
            log.info("上传失败");
            log.info("Error Message: " + ce.getMessage());
        } catch (Throwable e) {
            log.info("上传失败");
            log.info("Throwable Message: " + e.getMessage());
        } finally {
            pool.returnObject(ossClient);
            //ossClient.shutdown();
        }
        return filename;
    }

    /**
     * 下载文件，通过自己服务器中转
     *
     * @param uploadFile    远程路径
     * @return  本地路径
     */
    public String downFile(String uploadFile) {

        String filename = uploadFile.substring(uploadFile.lastIndexOf("/")+1, uploadFile.length());
        String localPath = getDownloadFile()+filename;
        OSSClient ossClient=null;
        try {
            ossClient = pool.borrowObject();
            checkFile(getDownloadFile());

            DownloadFileRequest downloadFileRequest = new DownloadFileRequest(getBucketName(), filename);
            // 设置本地文件
            downloadFileRequest.setDownloadFile(localPath);
            // 设置并发下载数，默认1
            downloadFileRequest.setTaskNum(getTaskNum());
            // 设置分片大小，默认100KB

            downloadFileRequest.setPartSize(getPartSize());
            // 开启断点续传，默认关闭
            downloadFileRequest.setEnableCheckpoint(getEnableCheckpoint());

            DownloadFileResult downloadResult = ossClient.downloadFile(downloadFileRequest);

            log.info("下载文件："+localPath);
            log.info("下载完成");
//            ObjectMetadata objectMetadata = downloadResult.getObjectMetadata();
//            System.out.println(objectMetadata.getETag());
//            System.out.println(objectMetadata.getLastModified());
//            System.out.println(objectMetadata.getUserMetadata().get("meta"));
        } catch (OSSException oe) {
            log.info("下载失败");
            log.info("Error Message: " + oe.getErrorCode());
        } catch (ClientException ce) {
            log.info("下载失败");
            log.info("Error Message: " + ce.getMessage());
        } catch (Throwable e) {
            log.info("下载失败");
            log.info("throws Message: " + e.getMessage());
        } finally {
            pool.returnObject(ossClient);
//            ossClient.shutdown();
        }
        return localPath;
    }

    /**
     *  删除文件
     * @param uploadFile
     * @return
     */
    public boolean delFile(String uploadFile) {

        String filename = uploadFile.substring(uploadFile.lastIndexOf("/")+1, uploadFile.length());
        boolean flag = false;
        OSSClient ossClient=null;
        try {
            ossClient = pool.borrowObject();

            boolean found = ossClient.doesObjectExist(getBucketName(), filename);
            if(found){
                ossClient.deleteObject(getBucketName(), filename);
                flag = true;
                log.info("删除完成");
            }

        } catch (OSSException oe) {
            log.info("删除失败");
            log.info("Error Message: " + oe.getErrorCode());
        } catch (ClientException ce) {
            log.info("删除失败");
            log.info("Error Message: " + ce.getMessage());
        } catch (Throwable e) {
            log.info("删除失败");
            log.info("throws Message: " + e.getMessage());
        } finally {
            pool.returnObject(ossClient);
//            ossClient.shutdown();
        }
        return flag;
    }

    static void checkFile(String path){
        File file = new File(path);
        if(!file.exists()){
            file.mkdirs();
        }
    }


    /**
     * 关闭   备用
     */
//    public void close() {
//        ossClient.shutdown();
//    }

    public static String getId() {
        String id = UUID.randomUUID().toString();
        id = id.replace("-", "");
        return id;
    }

    public static void main(String[] args) throws  Exception {

        OssUtil ou = new OssUtil();
//        ou.delFile("http://oss-cn-shanghai.aliyuncs.com/qz-oss-test/d2b567a08f1c46af8dcb0ea97fd7dab2.pdf");
//        ou.downFile("http://oss-cn-shanghai.aliyuncs.com/qz-oss-test/笑脸男.jpg");
        ou.uplFile("d:\\abc.pdf");


//        File file = new File("d:\\abc.pdf");
//        long fileSize = file.length();
//        FileInputStream fi = new FileInputStream(file);
//        byte[] file_buff = new byte[(int) fileSize];
//        int offset = 0;
//        int numRead = 0;
//        while (offset < file_buff.length && (numRead = fi.read(file_buff, offset, file_buff.length - offset)) >= 0) {
//            offset += numRead;
//        }
//        // 确保所有数据均被读取
//        if (offset != file_buff.length) {
//            throw new IOException("Could not completely read file "
//                    + file.getName());
//        }
//        ou.uplFileByBytes(file_buff);
//        fi.close();
    }



//    public static String getEndpoint() {
//        return endpoint;
//    }
//
//    public static void setEndpoint(String endpoint) {
//        OssUtil.endpoint = endpoint;
//    }
//
//    public static String getAccessKeyId() {
//        return accessKeyId;
//    }
//
//    public static void setAccessKeyId(String accessKeyId) {
//        OssUtil.accessKeyId = accessKeyId;
//    }
//
//    public static String getAccessKeySecret() {
//        return accessKeySecret;
//    }
//
//    public static void setAccessKeySecret(String accessKeySecret) {
//        OssUtil.accessKeySecret = accessKeySecret;
//    }
//
//    public static String getBucketName() {
//        return bucketName;
//    }
//
//    public static void setBucketName(String bucketName) {
//        OssUtil.bucketName = bucketName;
//    }
//
//    public static String getDownloadFile() {
//        return downloadFile;
//    }
//
//    public static void setDownloadFile(String downloadFile) {
//        OssUtil.downloadFile = downloadFile;
//    }

}
