package com.bebe.curator.cache;

import com.bebe.curator.cluster.Cluster;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

import java.util.ArrayList;
import java.util.Objects;

public class AgentCache extends AbstractChildrenCache{
    private Cluster cluster;

    public AgentCache(Cluster cluster){
        super(cluster.getClient(), cluster.getAgentNodePath());
        this.cluster = cluster;
    }

    @Override
    protected ListenerChildren getListener() {
        return new ListenerChildren(this);
    }

    private class ListenerChildren extends ChildrenCacheListener {

        public ListenerChildren(PathChildrenCache cache){
            super(cluster.getAgentNodePath(), cache);
        }

        @Override
        protected void process() {
            synchronized (children){
                log.info("\t=== Alive agents[{}]:{} ===", children.size(), Objects.toString(children));
                if(cluster.isMaster()) {
                    cluster.assignTask(new ArrayList<String>(children), null);
                }
            }
        }
    }
}
