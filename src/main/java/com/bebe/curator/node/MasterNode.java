package com.bebe.curator.node;

import com.bebe.curator.cache.Cache;
import com.bebe.curator.cache.MasterCache;
import com.bebe.curator.cluster.Cluster;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class MasterNode extends AbsractNode{

    private Cluster cluster;
    private AgentNode agentNode;
    private ConfNode confNode;
    private ProcessNode processNode;

    public MasterNode(Cluster cluster, AgentNode agentNode, ConfNode confNode, ProcessNode processNode){
        super(cluster.getMasterNodePath(), false, cluster.getClient());
        this.cluster = cluster;
        this.agentNode = agentNode;
        this.confNode = confNode;
        this.processNode = processNode;
        createMasterInfoNode();
    }

    @Override
    protected Cache getCache() {
        return new MasterCache(cluster, this);
    }

    @Override
    protected void afterCreated() {
        log.info("\t=== run as master. ===");
        cluster.setMaster(true);
        agentNode.listen();
        confNode.listen();
        processNode.listen();

        // addChildPath ../info/master/agentName
        try {
            cluster.getClient()
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(String.format("%s/%s", cluster.getInfoMasterPath(), cluster.getAgentName()));
        }catch (Exception e){
            handleException(e);
        }
    }

    @Override
    protected void handleNodeException(Exception e) {
        cluster.shutdown("MasterNode");
    }

    @Override
    protected void nodeExists() {
        //do nothing.
    }

    private void createMasterInfoNode(){
        try {
            cluster.getClient().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(cluster.getInfoMasterPath());
        }catch (KeeperException.NodeExistsException e){
            //do nothing.
        }catch (Exception e){
            handleException(e);
        }
    }
}
