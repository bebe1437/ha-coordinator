package com.bebe.curator.cluster;

import com.bebe.common.Configuration;
import com.bebe.common.Constants;
import com.bebe.common.Sleep;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class Agent {
    private static final Logger LOG = LoggerFactory.getLogger(Agent.class);
    private static final String NODE_PATH = String.format("%s/%s", Cluster.AGENT_NODE_PATH, Constants.AGENT_NAME);
    private static final int MAX_RETRIES = Configuration.getRetries();

    private Cluster cluster;
    private CuratorFramework client;
    private AtomicInteger retries = new AtomicInteger(0);

    public Agent(Cluster cluster, CuratorFramework client){
        this.cluster = cluster;
        this.client = client;
    }

    public void register(){
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withProtection()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(NODE_PATH);
            cluster.startProcess();
        }catch (KeeperException.NodeExistsException e){
            int retry = retries.incrementAndGet();
            if(retry<MAX_RETRIES){
                LOG.error("\t=== Retry to register Agent:{}. ===", NODE_PATH);
                Sleep.start();
                register();
            }else{
                LOG.error("\t=== Duplicated Agent:{} registered. ===", NODE_PATH);
                cluster.shutdown();
            }
        }catch (Exception e){
            LOG.error("\t=== registerAgent:{} ===", e);
            cluster.shutdown();
        }
    }
}
