package com.bebe.curator.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class AbstractChildrenCache extends PathChildrenCache {
    protected Logger log = LoggerFactory.getLogger(getClass());

    protected Set<String> children = new HashSet<String>();
    protected CuratorFramework client;
    private String parentPath;
    private String lockPath;


    public AbstractChildrenCache(CuratorFramework client, String path){
        super(client, path, true);
        this.client = client;
        this.parentPath = path;
        this.lockPath = String.format("%s_lock", parentPath);
    }

    @Override
    public void start() throws Exception{
        super.start();
        super.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                synchronized (children){

                    List<ChildData> childDataList = null;
                    log.info("\t=== acquire lock: {} ===", lockPath);
                    InterProcessMutex lock = new InterProcessMutex(client, lockPath);
                    try {
                        if(lock.acquire(3000l, TimeUnit.MICROSECONDS)){
                            childDataList = getCurrentData();
                        }
                    }catch (Exception e){
                        log.error("\t=== fail to do lock:{} ===", e);
                    }finally {
                        try {
                            lock.release();
                            log.info("\t=== release lock: {} ===", lockPath);
                        }catch (Exception e){
                            log.debug("\t=== release lock:{} ===", e);
                        }
                    }

                    if(childDataList == null){
                        childEvent(curatorFramework, pathChildrenCacheEvent);
                        return;
                    }

                    Set<String> tmp = new HashSet<String>();
                    for (ChildData childData : childDataList) {
                        String path = childData.getPath().replaceAll(parentPath+"/", "");
                        tmp.add(path);
                        if(children.contains(path)){
                            continue;
                        }
                        children.add(path);
                        log.info("\t=== add: {}  ===", path);
                    }

                    for(Iterator<String> it = children.iterator(); it.hasNext();){
                        String path = it.next();
                        if(!tmp.contains(path)){
                            it.remove();
                            log.info("\t=== remove: {} ===", path);
                        }
                    }

                    process();
                }
            }
        });
    }

    public void stop(){
        try {
            close();
        }catch (Exception e){
            log.error("\t=== stop: {} ===", e);
        }
    }

    protected abstract void process();
}
