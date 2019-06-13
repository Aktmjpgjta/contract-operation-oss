package com.pt.modules.oss.pool;


import com.aliyun.oss.OSSClient;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * oss链接池对象
 * 可以直接new 调用borrow方法调用链接
 * <p/>
 * return 返回使用
 */
public class OssConnectObjectPool extends GenericObjectPool<OSSClient> {

    public OssConnectObjectPool() {
        super(new OssConnectPoolObjectFactory(), (OssConnectConfig) SpringApplicationContext.getBean("ossConnectConfig"));
    }
    public OssConnectObjectPool(PooledObjectFactory<OSSClient> factory) {
        super(factory);
    }

    public OssConnectObjectPool(PooledObjectFactory<OSSClient> factory, GenericObjectPoolConfig config) {
        super(factory, config);
    }
    public OssConnectObjectPool(PooledObjectFactory<OSSClient> factory, GenericObjectPoolConfig config, AbandonedConfig abandonedConfig) {
        super(factory, config, abandonedConfig);
    }

    @Override
    public OSSClient borrowObject() throws Exception {
        return super.borrowObject();
    }

    @Override
    public void returnObject(OSSClient obj) {
        super.returnObject(obj);
    }
}
