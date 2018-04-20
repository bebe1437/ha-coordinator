package com.bebe.curator.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class AbstractChildrenCache {
    protected Logger log = LoggerFactory.getLogger(getClass());

    private PathChildrenCache cache;
    protected Set<String> children = new HashSet<String>();
    protected CuratorFramework client;
    private String parentPath;

    public AbstractChildrenCache(CuratorFramework client, String path){
        this.client = client;
        this.parentPath = path;
        cache = new PathChildrenCache(client, path, true);
    }

    public void start() throws Exception{
        cache.start();
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                synchronized (children){
                    List<ChildData> childDataList = cache.getCurrentData();
                    Set<String> tmp = new HashSet<String>();
                    for (ChildData childData : childDataList) {
                        String path = childData.getPath().replaceAll(parentPath+"/", "");
                        tmp.add(path);
                        if(children.contains(path)){
                            continue;
                        }
                        children.add(path);
                        log.info("\t=== add:{}  ===", path);
                    }

                    for(Iterator<String> it = children.iterator(); it.hasNext();){
                        String path = it.next();
                        if(!tmp.contains(path)){
                            it.remove();
                            log.info("\t=== remove:{} ===", path);
                        }
                    }

                    process();
                }
            }
        });
    }

    public void stop(){
        try {
            cache.close();
        }catch (Exception e){
            log.error("\t=== stop:{} ===", e);
        }
    }

    protected abstract void process();
}
