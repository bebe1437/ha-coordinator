package com.bebe.curator.node;

import com.bebe.curator.cache.Cache;
import com.bebe.curator.cache.TaskCache;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.process.Processor;

public class TaskNode extends AbsractNode{

    private Cluster cluster;
    private Processor processor;

    public TaskNode(Cluster cluster, Processor processor){
        super(cluster.getInfoTaskPath(), true, cluster.getClient());
        this.cluster = cluster;
        this.processor = processor;
    }

    @Override
    protected Cache getCache() {
        return new TaskCache(cluster, processor);
    }

    @Override
    protected void handleNodeException(Exception e) {
        cluster.shutdown("TaskNode");
    }

    @Override
    protected void nodeExists() {
        //do nothing.
    }
}
