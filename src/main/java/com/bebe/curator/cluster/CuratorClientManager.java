package com.bebe.curator.cluster;

import com.bebe.common.Configuration;
import com.bebe.common.Constants;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorClientManager {
    private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);
    private static final String ZK_HOST;
    private static final int SESSION_TIMEOUT;
    static{
        ZK_HOST = Configuration.getZKHost();
        SESSION_TIMEOUT = Configuration.getSessionTimeout();
    }



    public static CuratorFramework start(ConnectionStateListener listener){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry((int) Constants.BUFFER_TIME, Configuration.getRetries());
        CuratorFramework client = CuratorFrameworkFactory
                .builder()
                .connectString(ZK_HOST)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .build();
        client.getConnectionStateListenable().addListener(listener);
        client.start();
        return client;
    }
}
