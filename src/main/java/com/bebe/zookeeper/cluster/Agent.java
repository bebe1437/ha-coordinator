package com.bebe.zookeeper.cluster;

import com.bebe.zookeeper.common.Configuration;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Agent {
    private static final Logger LOG = LoggerFactory.getLogger(Agent.class);
    private static final String NODE_NAME = Configuration.getAgentName();
    private static final String NODE_PATH = String.format("%s/%s", Cluster.AGENT_NODE_PATH, NODE_NAME);
    private static final int MAX_RETRIES = Configuration.getRetries();

    private ZooKeeper zk;
    private Random random;
    private Cluster cluster;
    private AtomicInteger retries = new AtomicInteger(0);

    public Agent(Cluster cluster, ZooKeeper zk){
        this.cluster = cluster;
        this.zk = zk;
        random = new Random();
    }

    public void register(){
        zk.create(NODE_PATH
                , Integer.toHexString(random.nextInt()).getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE
                , CreateMode.EPHEMERAL
                , new AsyncCallback.StringCallback() {

                    public void processResult(int rc, String path, Object ctx, String name) {
                        switch (KeeperException.Code.get(rc)) {
                            case OK:
                                LOG.info("\t=== Register Agent: {} successfully. ===", name);
                                cluster.startProcess();
                                break;
                            case CONNECTIONLOSS:
                                register();
                                break;
                            case NODEEXISTS:
                                int retry = retries.incrementAndGet();
                                if(retry == MAX_RETRIES) {
                                    LOG.error("\t=== Duplicated Agent:{} registered. ===", path);
                                    cluster.shutdown();
                                }else{
                                    LOG.error("\t=== Retry to register Agent:{}. ===", path);
                                    register();
                                }
                                break;
                            default:
                                LOG.error("\t=== Fail to register [{}]:{} ===", rc, KeeperException.create(KeeperException.Code.get(rc), path));
                                cluster.shutdown();
                        }
                    }

                }
                , null);
    }
}
