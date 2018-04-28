package com.bebe.curator.cache;

import com.bebe.common.Constants;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.node.ConfNode;

public class ConfCache extends AbstractNodeCache{

    private ConfNode confNode;
    private Cluster cluster;

    public ConfCache(Cluster cluster, ConfNode confNode){
        super(cluster.getClient(), cluster.getConfNodePath());
        this.confNode = confNode;
        this.cluster = cluster;
    }

    @Override
    public void process(byte[] data) {
        confNode.update(new String(data, Constants.UTF8));
    }

    @Override
    public void remove() {
        log.warn("\t=== {} was removed. ===", cluster.getConfNodePath());
    }
}
