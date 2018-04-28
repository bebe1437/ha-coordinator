package com.bebe.curator.node;

import com.bebe.common.Constants;
import com.bebe.curator.cache.Cache;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public abstract class AbsractNode implements Node{
    protected Logger log = LoggerFactory.getLogger(getClass());
    private CuratorFramework client;
    private String nodePath;
    private boolean isPersisten;
    protected Cache cache;
    protected Charset charset = Constants.UTF8;

    public AbsractNode(String nodePath, boolean isPersisten, CuratorFramework client){
        this.nodePath = nodePath;
        this.isPersisten = isPersisten;
        this.client = client;
    }

    @Override
    public void listen() {
        if(cache == null) {
            cache = getCache();
        }
        try {
            cache.listen();
        }catch (Exception e){
            handleException(e);
        }
    }

    @Override
    public void stop(){
        if(cache!=null){
            CloseableUtils.closeQuietly(cache);
        }
    }

    protected abstract void handleNodeException(Exception e);
    protected abstract void nodeExists();
    protected Cache getCache(){ return null;}
    protected void afterCreated(){}

    protected void handleException(Exception e){
        log.error("\t=== e:{} ===", e);
        handleNodeException(e);
    }

    @Override
    public void create() {
        try{
            client.create()
                .creatingParentsIfNeeded()
                .withMode(isPersisten? CreateMode.PERSISTENT: CreateMode.EPHEMERAL)
                .forPath(nodePath);
            afterCreated();
        }catch (KeeperException.NodeExistsException e){
            nodeExists();
        }catch (Exception e){
            handleException(e);
        }
    }

    @Override
    public void setData(byte[] data) {
        if(!exists()){
            try {
                client.create()
                        .creatingParentsIfNeeded()
                        .forPath(nodePath, data);
            }catch (KeeperException.NodeExistsException e){
                setData(data);
            }catch (Exception e){
                handleException(e);
            }
        }else{
            try {
                client.setData().forPath(nodePath, data);
            }catch (Exception e){
                handleException(e);
            }
        }
    }

    @Override
    public boolean exists(){
        try {
            return client.checkExists().forPath(nodePath) != null;
        }catch (Exception e){
            handleException(e);
            return false;
        }
    }
}
