package com.bebe.curator.node;

import com.bebe.curator.cache.AgentCache;
import com.bebe.curator.cache.Cache;
import com.bebe.curator.cluster.Cluster;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class AgentNode extends AbsractNode{

    private Cluster cluster;
    private int registerCount = 0;

    public AgentNode(Cluster cluster){
        super(cluster.getAgentNodePath(), true, cluster.getClient());
        this.cluster = cluster;
    }

    @Override
    protected Cache getCache() {
        return new AgentCache(cluster);
    }

    @Override
    protected void handleNodeException(Exception e) {
        cluster.shutdown("AgentNode");
    }

    @Override
    protected void nodeExists() {
        register();
    }

    @Override
    protected void afterCreated(){
        register();
    }

    private void register(){
        try {
            cluster.getClient().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(String.format("%s/%s", cluster.getAgentNodePath(), cluster.getAgentName()));
        }catch (KeeperException.NodeExistsException e){
            if(++registerCount>cluster.getMaxRetries()){
                log.error("\t=== Duplicate agent registered. ===");
                cluster.shutdown("DuplicateAgentRegistered");
                return;
            }
            register();
        }catch (Exception e){
            handleException(e);
        }
    }
}
