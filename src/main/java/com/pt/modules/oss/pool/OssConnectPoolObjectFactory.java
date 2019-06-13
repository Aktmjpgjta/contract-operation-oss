package com.pt.modules.oss.pool;


import com.aliyun.oss.OSSClient;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 链接对象生成工厂
 * 提供生成方法，create
 * 回归链接时会嗲用到 wrap方法
 */
public class OssConnectPoolObjectFactory extends BasePooledObjectFactory<OSSClient> {


    static Logger log = LoggerFactory.getLogger(OssConnectPoolObjectFactory.class);
    private OssConnectConfig config = (OssConnectConfig) SpringApplicationContext.getBean("ossConnectConfig");
    @Override
    public OSSClient create() throws Exception {
        return new OSSClient(config.getEndpoint(), config.getAccessKeyId(), config.getAccessKeySecret());
    }

    @Override
    public PooledObject<OSSClient> wrap(OSSClient obj) {
        return new DefaultPooledObject(obj);
    }
}
