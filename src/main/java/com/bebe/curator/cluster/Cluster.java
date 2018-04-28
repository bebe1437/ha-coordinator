package com.bebe.curator.cluster;

import com.bebe.common.Config;
import com.bebe.curator.node.*;
import com.bebe.curator.process.ArrangeProcessHandler;
import com.bebe.curator.process.Processor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Cluster{
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

    private CuratorFramework client;
    private Boolean isShutdown = false;
    private Boolean isMaster = false;

    private ClusterFactory.Builder builder;
    private String clusterNodePath;
    private String agentNodePath;
    private String processNodePath;
    private String masterNodePath;
    private String confNodePath;
    private String infoNodePath;
    private String infoMasterPath;
    private String infoTaskPath;
    private String processLockPath;

    private ConnectionStateListener listener;

    private Processor processor;
    private AgentNode agentNode;
    private ProcessNode processNode;
    private MasterNode masterNode;
    private ConfNode confNode;
    private TaskNode taskNode;


    private ArrangeProcessHandler arrangeProcessHandler;

    public Cluster(ClusterFactory.Builder builder, CuratorFramework client){
        this.builder = builder;
        this.client = client;

        clusterNodePath = String.format("/%s", builder.getClusterName());
        agentNodePath = String.format("%s/agents", clusterNodePath);
        processNodePath = String.format("%s/processes", clusterNodePath);
        masterNodePath = String.format("%s/master", clusterNodePath);
        confNodePath = String.format("%s/conf", clusterNodePath);

        processLockPath = String.format("%s_lock", processNodePath);

        infoNodePath = String.format("%s/info", clusterNodePath);
        infoMasterPath = String.format("%s/master", infoNodePath);
        infoTaskPath = String.format("%s/task", infoNodePath);

        listener = new StateListener("cluster", this);
    }

    public void start(){
        client.getConnectionStateListenable().addListener(listener);
        agentNode = new AgentNode(this);
        processNode = new ProcessNode(this);
        confNode = new ConfNode(this);
        processor = new Processor(this);
        taskNode = new TaskNode(this, processor);
        masterNode = new MasterNode(this, agentNode, confNode, processNode);
        arrangeProcessHandler = new ArrangeProcessHandler(this, taskNode);

        agentNode.create();
        processNode.create();
        confNode.init();

        taskNode.create();
        taskNode.listen();
        masterNode.create();
        masterNode.listen();
    }

    public boolean isMaster(){
        return isMaster;
    }

    public synchronized void setMaster(boolean isMaster){
        this.isMaster = isMaster;
    }


    public void assignTask(List<String> agents, List<String> processes){
        assignTask(confNode.getConf().getCommand(), agents, processes);
    }
    public void assignTask(String command, List<String> agents, List<String> processes){
        arrangeProcessHandler.process(command, agents, processes);
    }

    public void shutdown(String who){
        LOG.info("\t==={}: shutdown ===", who);

        agentNode.stop();
        confNode.stop();
        masterNode.stop();
        taskNode.stop();
        processNode.stop();

        taskNode = null;
        masterNode = null;
        confNode = null;
        agentNode = null;
        processNode = null;
        arrangeProcessHandler = null;

        isShutdown = true;
        processor.stop("shutdown");
        CloseableUtils.closeQuietly(client);
    }

    public void restart(){
        processor.stop("restart");
        start();
    }

    public Boolean isShutdown(){
        return isShutdown;
    }

    public String getZKHost(){
        return builder.getZkHost();
    }

    public int getSessionTimeout(){
        return builder.getSessionTimeout();
    }

    public String getAgentNodePath() {
        return agentNodePath;
    }

    public String getMasterNodePath() { return masterNodePath; }

    public String getProcessNodePath() {
        return processNodePath;
    }

    public String getConfNodePath(){ return confNodePath; }

    public String getInfoMasterPath(){ return infoMasterPath; }

    public String getInfoTaskPath(){ return infoTaskPath; }

    public String getProcessLockPath(){ return processLockPath; }

    public String getAgentName(){
        return builder.getAgentName();
    }

    public int getMaxRetries(){
        return builder.getMaxRetries();
    }

    public CuratorFramework getClient(){
        return client;
    }

    public long getBufferTime(){
        return builder.getBufferTime();
    }

    public String getKill(){ return builder.getKill(); }

    public Processor getProcessor(){
        return processor;
    }

    public ClusterFactory.Builder getBuilder(){
        return builder;
    }

    public Config getConf(){
        return confNode.getConf();
    }
}
