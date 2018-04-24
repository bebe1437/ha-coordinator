package com.bebe.curator.cluster;

import com.bebe.curator.cache.AgentCache;
import com.bebe.curator.cache.ProcessCache;
import com.bebe.curator.process.Processor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster{
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

    private CuratorFramework client;
    private Processor processor;
    private AgentCache agentCache;
    private ProcessCache processCache;
    private ConfigManager configManager;
    private Boolean isShutdown = false;

    private ClusterFactory.Builder builder;
    private String clusterNodePath;
    private String agentNodePath;
    private String processNodePath;

    private ConnectionStateListener listener;

    public Cluster(ClusterFactory.Builder builder, CuratorFramework client){
        this.builder = builder;
        this.client = client;

        clusterNodePath = String.format("/%s", builder.getClusterName());
        agentNodePath = String.format("%s/agents", clusterNodePath);
        processNodePath = String.format("%s/processes", clusterNodePath);

        listener = new StateListener("cluster", this);
    }

    public void start(){
        client.getConnectionStateListenable().addListener(listener);

        createAgentNode();

        configManager = new ConfigManager(this);
        processor = new Processor(this, configManager);
        configManager.init();

        createProcessNode(configManager);

        new Agent(this).register();
    }

    private void createAgentNode(){
        LOG.info("createAgentNode");
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(agentNodePath);
        }catch (KeeperException.NodeExistsException e){
        }catch (Exception e){
            LOG.error("\t=== createAgentNode:{} ===", e);
        }
        agentCache = new AgentCache(client, agentNodePath);

        try {
            agentCache.start();
        }catch (Exception e){
            LOG.error("\t=== agentCache:{} ===", e);
        }
    }

    private void createProcessNode(ConfigManager configManager){
        LOG.info("createProcessNode");
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(processNodePath);
        }catch (KeeperException.NodeExistsException e){
        }catch (Exception e){
            LOG.error("\t=== createProcessNode:{} ===", e);
        }
        processCache = new ProcessCache(this, processor, configManager);

        try{
            processCache.start();
        }catch (Exception e){
            LOG.error("\t=== processCache:{} ===", e);
        }
    }

    public void shutdown(String who){
        LOG.info("\t==={}: shutdown ===", who);

        CloseableUtils.closeQuietly(processCache);
        CloseableUtils.closeQuietly(agentCache);
        CloseableUtils.closeQuietly(client);
        synchronized (isShutdown){
            isShutdown = true;
            processor.stop("shutdown");
        }
    }

    public synchronized Boolean isShutdown(){
        return isShutdown;
    }

    public void startProcess(){
        processor.start();
    }

    public void recheckMaximumProcessors(){
        processCache.recheck();
    }

    public String getZKHost(){
        return builder.getZkHost();
    }

    public int getSessionTimeout(){
        return builder.getSessionTimeout();
    }

    public String getClusterNodePath() {
        return clusterNodePath;
    }

    public String getAgentNodePath() {
        return agentNodePath;
    }

    public String getProcessNodePath() {
        return processNodePath;
    }

    public String getAgentName(){
        return builder.getAgentName();
    }

    public int getMaxRetries(){
        return builder.getMaxRetries();
    }

    public CuratorFramework getClient(){
        return client;
    }

    public String getCommand(){
        return builder.getCommand();
    }

    public int getMaxProcessors(){
        return builder.getMaxProcessors();
    }

    public long getBufferTime(){
        return builder.getBufferTime();
    }

    public void restart(){
        processor.stop("restart");
        configManager.stop();
        CloseableUtils.closeQuietly(processCache);
        CloseableUtils.closeQuietly(agentCache);
        start();
    }
}
