package com.bebe.curator.cluster;

import com.bebe.common.Sleep;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class Agent {
    private static final Logger LOG = LoggerFactory.getLogger(Agent.class);

    private String nodePath;
    private Cluster cluster;
    private CuratorFramework client;
    private AtomicInteger retries = new AtomicInteger(0);

    public Agent(Cluster cluster){
        this.cluster = cluster;
        this.client = cluster.getClient();
        nodePath = String.format("%s/%s", cluster.getAgentNodePath(), cluster.getAgentName());
    }

    //TODO check duplicated agent withProtection
    public void register(){
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    //.withProtection()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(nodePath);
            //cluster.startProcess();
        }catch (KeeperException.NodeExistsException e){
            int retry = retries.incrementAndGet();
            if(retry<cluster.getMaxRetries()){
                LOG.error("\t=== Retry to register Agent:{}. ===", nodePath);
                Sleep.start();
                register();
            }else{
                LOG.error("\t=== Duplicated Agent:{} registered. ===", nodePath);
                cluster.shutdown("Agent-register");
            }
        }catch (Exception e){
            LOG.error("\t=== registerAgent:{} ===", e);
            cluster.shutdown("Agent-register-exception");
        }
    }
}
