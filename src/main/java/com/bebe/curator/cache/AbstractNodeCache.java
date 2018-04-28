package com.bebe.curator.cache;

import com.bebe.common.Constants;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public abstract class AbstractNodeCache extends NodeCache implements com.bebe.curator.cache.NodeCache{
    protected Logger log = LoggerFactory.getLogger(getClass());
    protected Charset charset = Constants.UTF8;

    public AbstractNodeCache(CuratorFramework client, String path){
        super(client, path);
    }

    @Override
    public void listen() throws Exception{
        super.start();
        this.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                log.info("\t=== nodeChanged. ===");
                if(getCurrentData()!=null){
                    process(getCurrentData().getData());
                }else{
                    remove();
                }
            }
        });
    }

    public void stop(){
        CloseableUtils.closeQuietly(this);
    }

}
