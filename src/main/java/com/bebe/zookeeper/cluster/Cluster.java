package com.bebe.zookeeper.cluster;

import com.bebe.zookeeper.common.Configuration;
import com.bebe.zookeeper.common.ExitError;
import com.bebe.zookeeper.process.Processor;
import com.bebe.zookeeper.watcher.NodesWatcher;
import com.bebe.zookeeper.watcher.ProcessorsWatcher;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class Cluster {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private static final String CLUSTER_NAME = Configuration.getClusterName();
    private static final String ZK_HOST = Configuration.getZKHost();
    private static final int SESSION_TIMEOUT = Configuration.getSessionTimeout();

    public static final String CLUSTER_NODE_PATH;
    public static final String AGENT_NODE_PATH;
    public static final String PROCESS_NODE_PATH;

    static{
        CLUSTER_NODE_PATH = String.format("/%s", CLUSTER_NAME);
        AGENT_NODE_PATH = String.format("%s/agents", CLUSTER_NODE_PATH);
        PROCESS_NODE_PATH = String.format("%s/processes", CLUSTER_NODE_PATH);
    }

    private ZookeeperManager zookeeperManager;
    private ZooKeeper zk;
    private ConfigManager configManager;
    private Processor processor;
    private ProcessorsWatcher processorsWatcher;

    private Random random;

    public Cluster(){
        zookeeperManager = new ZookeeperManager(ZK_HOST, SESSION_TIMEOUT);
        zk = zookeeperManager.start();
        if(zk == null){
            LOG.error("\t=== fail to start zookeeper conneciton. ===");
            System.exit(ExitError.ZK_CONNECTION.getCode());
        }

        random = new Random();

    }

    public void shutdown(){
        zookeeperManager.stop();
        System.exit(ExitError.SHUTDOWN.getCode());
    }

    public void start(){
        createMainNode();
    }

    private void createMainNode(){
        zk.create(CLUSTER_NODE_PATH
                , Integer.toHexString(random.nextInt()).getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE
                , CreateMode.PERSISTENT
                , callback
                , null);
    }

    private void createAgentNode(){
        zk.create(AGENT_NODE_PATH
                , Integer.toHexString(random.nextInt()).getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE
                , CreateMode.PERSISTENT
                , callback
                , null);

    }

    private void createProcessNode(){
        zk.create(PROCESS_NODE_PATH
                , Integer.toHexString(random.nextInt()).getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE
                , CreateMode.PERSISTENT
                , callback
                , null);
    }

    AsyncCallback.StringCallback callback = new AsyncCallback.StringCallback() {

        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.info("\t=== Initial {} successfully. ===", name);
                    next(path);
                    break;
                case CONNECTIONLOSS:
                    restart(path);
                    break;
                case NODEEXISTS:
                    next(path);
                    break;
                default:
                    LOG.error("\t=== Fail to register [{}]:{} ===", rc, KeeperException.create(KeeperException.Code.get(rc), path));
                    shutdown();
            }
        }

    };

    private void next(String path){
        if(path.equals(CLUSTER_NODE_PATH)){
            createAgentNode();
        }else if(path.equals(AGENT_NODE_PATH)){
            createProcessNode();
        }else if(path.equals(PROCESS_NODE_PATH)){
            process();
        }
    }

    private void restart(String path){
        if(path.equals(CLUSTER_NODE_PATH)){
            createMainNode();
        }else if(path.equals(AGENT_NODE_PATH)){
            createAgentNode();
        }else if(path.equals(PROCESS_NODE_PATH)){
            createProcessNode();
        }
    }

    private void process(){
        configManager = new ConfigManager(this, zk);
        processor = new Processor(configManager);

        // initial config
        configManager.initial();
        processorsWatcher = new ProcessorsWatcher(zk , processor, configManager);
        // start process's watcher
        watchProcess(processorsWatcher);

        // start agent's watcher
        watchAgent();
        // start agent
        new Agent(this, zk).register();
    }

    private void watchAgent(){
        try {
            zk.getChildren(Cluster.AGENT_NODE_PATH, new NodesWatcher(zk));
        }catch (KeeperException | InterruptedException e){
            LOG.error("\t=== watchApp error:{} ===", e);
            watchAgent();
        }
    }

    private void watchProcess(ProcessorsWatcher processorsWatcher){
        try {

            zk.getChildren(Cluster.PROCESS_NODE_PATH, processorsWatcher);
        }catch (KeeperException | InterruptedException e){
            LOG.error("\t=== watchApp error:{} ===", e);
            watchProcess(processorsWatcher);
        }
    }

    public void startProcess(){
        processor.start();
    }

    public void checkAliveProcessors(){
        processorsWatcher.check(PROCESS_NODE_PATH);
    }
}
