package com.bebe.curator.node;

import com.bebe.curator.cache.Cache;
import com.bebe.curator.cache.ProcessCache;
import com.bebe.curator.cluster.Cluster;

public class ProcessNode extends AbsractNode{

    private Cluster cluster;

    public ProcessNode(Cluster cluster){
        super(cluster.getProcessNodePath(), true, cluster.getClient());
        this.cluster = cluster;
    }

    @Override
    protected Cache getCache() {
        return new ProcessCache(cluster);
    }

    @Override
    protected void handleNodeException(Exception e) {
        cluster.shutdown("ProcessNode");
    }

    @Override
    protected void nodeExists() {
        //do nothing
    }

    @Override
    protected void afterCreated(){
       // do nothing
    }
}
