package com.bebe.curator.cluster;

import com.bebe.common.Constants;
import com.bebe.common.ExitError;
import com.bebe.curator.cache.AgentCache;
import com.bebe.curator.cache.ProcessCache;
import com.bebe.curator.process.Processor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster{
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    public static final String CLUSTER_NODE_PATH;
    public static final String AGENT_NODE_PATH;
    public static final String PROCESS_NODE_PATH;

    static{
        CLUSTER_NODE_PATH = String.format("/%s", Constants.CLUSTER_NAME);
        AGENT_NODE_PATH = String.format("%s/agents", CLUSTER_NODE_PATH);
        PROCESS_NODE_PATH = String.format("%s/processes", CLUSTER_NODE_PATH);
    }

    private CuratorFramework client;
    private Processor processor;
    private AgentCache agentCache;
    private ProcessCache processCache;
    public Cluster(){
        client = CuratorClientManager.start(new StateListener("cluster"));
    }

    public void shutdown(){
        client.close();
        System.exit(ExitError.SHUTDOWN.getCode());
    }

    public void start(){
        ConfigManager configManager = new ConfigManager(this, client);
        processor = new Processor(configManager);
        configManager.init();

        createAgentNode();
        createProcessNode(configManager);

        new Agent(this, client).register();
    }

    private void createAgentNode(){
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(AGENT_NODE_PATH);
        }catch (KeeperException.NodeExistsException e){
        }catch (Exception e){
            LOG.error("\t=== createAgentNode:{} ===", e);
        }
        agentCache = new AgentCache(client, AGENT_NODE_PATH);

        try {
            agentCache.start();
        }catch (Exception e){
            LOG.error("\t=== agentCache:{} ===", e);
        }
    }

    private void createProcessNode(ConfigManager configManager){
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(PROCESS_NODE_PATH);
        }catch (KeeperException.NodeExistsException e){
        }catch (Exception e){
            LOG.error("\t=== createProcessNode:{} ===", e);
        }
        processCache = new ProcessCache(client, processor, configManager);

        try{
            processCache.start();
        }catch (Exception e){
            LOG.error("\t=== processCache:{} ===", e);
        }
    }

    public void startProcess(){
        processor.start();
    }

    public void recheckMaximumProcessors(){
        processCache.recheck();
    }
}
