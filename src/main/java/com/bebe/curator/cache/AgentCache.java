package com.bebe.curator.cache;

import org.apache.curator.framework.CuratorFramework;

import java.util.Objects;

public class AgentCache extends AbstractChildrenCache{

    public AgentCache(CuratorFramework client, String path){
        super(client, path);
    }

    @Override
    protected void process() {
        synchronized (children){
            log.info("\t=== Alive agents[{}]:{} ===", children.size(), Objects.toString(children));
        }
    }
}
