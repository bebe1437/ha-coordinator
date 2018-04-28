package com.bebe.curator.cache;

import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.node.MasterNode;
import org.apache.zookeeper.KeeperException;

import java.util.List;

public class MasterCache extends AbstractNodeCache{

    private Cluster cluster;
    private MasterNode masterNode;

    public MasterCache(Cluster cluster, MasterNode masterNode){
        super(cluster.getClient(), cluster.getMasterNodePath());
        this.cluster = cluster;
        this.masterNode = masterNode;
    }

    @Override
    public void process(byte[] data) {
        if(cluster.isMaster()){
            cluster.assignTask(cluster.getConf().getCommand(), null, null);
        }else{
            checkWhoIsMaster();
        }
    }

    //in case connection issue
    private void checkWhoIsMaster(){
        try {
            List<String> children =  cluster.getClient().getChildren().forPath(cluster.getInfoMasterPath());
            cluster.setMaster(children.contains(cluster.getAgentName()));
            if(cluster.isMaster()){
                cluster.assignTask(cluster.getConf().getCommand(), null, null);
            }
        }catch (KeeperException.NoNodeException e){
        }catch (Exception e){
            log.error("\t=== checkWhoIsMaster:{} ===", e);
            cluster.shutdown("checkWhoIsMaster");
        }
    }

    @Override
    public void remove() {
        log.warn("\t=== {} was removed. ===", cluster.getMasterNodePath());
        masterNode.create();
    }
}
