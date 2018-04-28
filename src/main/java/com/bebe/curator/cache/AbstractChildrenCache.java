package com.bebe.curator.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractChildrenCache extends PathChildrenCache implements Cache{
    protected Logger log = LoggerFactory.getLogger(getClass());

    public AbstractChildrenCache(CuratorFramework client, String path){
        super(client, path, true);
    }

    protected abstract <LISTENER extends ChildrenCacheListener> LISTENER getListener();

    @Override
    public void listen() throws Exception{
        super.start();
        super.getListenable().addListener(this.getListener());
    }

    public void stop(){
        CloseableUtils.closeQuietly(this);
    }
}
