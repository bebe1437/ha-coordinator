package com.bebe.curator.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractNodeCache extends NodeCache{
    protected Logger log = LoggerFactory.getLogger(getClass());
    protected String data = "";

    public AbstractNodeCache(CuratorFramework client, String path){
        super(client, path);
    }

    public void start() throws Exception{
        super.start();
        this.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                log.info("\t=== nodeChanged. ===");
                synchronized (data){
                    if(getCurrentData()!=null){
                        process(getCurrentData().getData());
                    }else{
                        remove();
                    }
                }
            }
        });
    }

    public void stop(){
        try {
            close();
        }catch (Exception e){
            log.error("\t=== stop:{} ===", e);
        }
    }

    protected abstract void process(byte[] data);
    protected abstract void remove();
}
