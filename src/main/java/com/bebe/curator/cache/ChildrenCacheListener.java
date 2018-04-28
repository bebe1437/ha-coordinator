package com.bebe.curator.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class ChildrenCacheListener implements PathChildrenCacheListener {
    private Logger log = LoggerFactory.getLogger(getClass());

    private String parentPath;
    private PathChildrenCache cache;
    protected Set<String> children = new HashSet<String>();

    public ChildrenCacheListener(String parentPath, PathChildrenCache cache){
        this.parentPath = parentPath;
        this.cache = cache;
    }

    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
        synchronized (children){

            List<ChildData> childDataList = cache.getCurrentData();
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

    protected abstract void process();
}
