package com.bebe.curator.cache;

import com.bebe.curator.cluster.Cluster;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

import java.util.ArrayList;
import java.util.Objects;

public class ProcessCache extends AbstractChildrenCache{
    private Cluster cluster;

    public ProcessCache(Cluster cluster){
        super(cluster.getClient(), cluster.getProcessNodePath());
        this.cluster = cluster;
    }

    @Override
    protected ListenerChildren getListener() {
        return new ListenerChildren(this);
    }

    private class ListenerChildren extends ChildrenCacheListener {

        public ListenerChildren(PathChildrenCache cache){
            super(cluster.getProcessNodePath(), cache);
        }

        @Override
        protected void process() {
            log.info("\t=== Alive processors[{}]:{} ===", children.size(), Objects.toString(children));
            if(cluster.isMaster()){
                int max = cluster.getConf().getMaxProcessors();
                synchronized (children) {
                    if (children.size() < max) {
                        cluster.assignTask(null, new ArrayList<String>(children));
                    }
                }
            }
        }
    }
}
