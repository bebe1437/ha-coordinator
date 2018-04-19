package com.bebe.zookeeper.process;

import com.bebe.zookeeper.cluster.Cluster;
import com.bebe.zookeeper.cluster.ConfigManager;
import com.bebe.zookeeper.cluster.ZookeeperManager;
import com.bebe.zookeeper.common.Configuration;
import com.bebe.zookeeper.common.Sleep;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Processor {
    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);
    private static final String ZK_HOST = Configuration.getZKHost();
    private static final int SESSION_TIMEOUT = Configuration.getSessionTimeout();
    private static final String LOCK_NODE = String.format("%s/LOCK", Cluster.CLUSTER_NODE_PATH);
    private static final String PROCESS_NODE = String.format("%s/%s", Cluster.PROCESS_NODE_PATH, Configuration.getAgentName());
    private static final String PROCESS_PID_FILE = "process.pid";

    private Random random;
    private Process process;
    private ZookeeperManager zookeeperManager;
    private ZooKeeper zk;
    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    private ConfigManager configManager;

    public Processor(ConfigManager configManager){
        random = new Random();
        this.configManager = configManager;
        zookeeperManager = new ZookeeperManager(ZK_HOST, SESSION_TIMEOUT);
    }

    public void start(){
        if(zk !=null){
            stop();
        }
        zk = zookeeperManager.start();

        zk.create(LOCK_NODE
                , Integer.toHexString(random.nextInt()).getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE
                , CreateMode.EPHEMERAL
                , new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        switch (KeeperException.Code.get(rc)) {
                            case OK:
                                checkAliveProcessors();
                                releaseLock();
                                break;
                            case CONNECTIONLOSS:
                                start();
                                break;
                            case NODEEXISTS:
                                LOG.warn("\t=== Lock exists. ===");
                                stop();
                                break;
                            default:
                                LOG.error("\t=== Fail to create node[{}]:{} ===", path, KeeperException.create(KeeperException.Code.get(rc)));
                                start();
                        }
                    }
                }
                , null);
    }

    private void releaseLock(){
        zk.delete(LOCK_NODE, -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                switch (KeeperException.Code.get(rc)){
                    case OK:
                        break;
                    case CONNECTIONLOSS:
                        releaseLock();
                        break;
                    case NONODE:
                        break;
                    default:
                        LOG.error("\t=== Fail to release lock[{}]:{} ===", path, KeeperException.create(KeeperException.Code.get(rc)));
                        stop();
                }

            }
        }, null);
    }

    private void checkAliveProcessors(){
        try {
            List<String> liveProcessors = zk.getChildren(Cluster.PROCESS_NODE_PATH, false);
            int max = configManager.getMaxProcessors();
            if (liveProcessors.size() < max) {
                registerProcess();
            } else {
                LOG.info("\t=== Alive processors already reach maximum:{} ===", max);
            }
        }catch (Exception e){
            LOG.error("\t=== Fail to retrieve children:{} ===", e);
            Sleep.start();
            checkAliveProcessors();
        }
    }

    private void registerProcess(){
        zk.create(PROCESS_NODE
                , Integer.toHexString(random.nextInt()).getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE
                , CreateMode.EPHEMERAL
                , new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        switch (KeeperException.Code.get(rc)){
                            case OK:
                                runProcess();
                                break;
                            case NODEEXISTS:
                                LOG.error("\t=== Duplicated process node register. ===");
                                stop();
                                break;
                            case CONNECTIONLOSS:
                                registerProcess();
                                break;
                            default:
                                LOG.error("\t=== Fail to register [{}]:{} ===", path, KeeperException.create(KeeperException.Code.get(rc)));
                                stop();
                                break;
                        }
                    }
                }
                , null);
    }

    private void runProcess(){
        if(configManager == null){
            LOG.info("\t=== configManager not ready. ===");
            Sleep.start();
            runProcess();
            return;
        }
        String command = configManager.getCommand();
        try {
            LOG.info("\t=== exec command:{} ===", command);
            process = Runtime.getRuntime().exec(command);
            outputProcessID();
            startMonitor();
        }catch (IOException e){
            LOG.error("\t=== Fail to start com.bebe.zookeeper.process:{}. ===", e);
            zookeeperManager.stop();
        }
    }

    private void outputProcessID(){
        try {
            Field pidField = process.getClass().getDeclaredField("pid");
            pidField.setAccessible(true);
            Long pid = pidField.getLong(process);
            pidField.setAccessible(false);
            LOG.info("\t=== process id:{} ===", pid);

            FileWriter writer = new FileWriter(PROCESS_PID_FILE);
            writer.write(String.valueOf(pid));
            writer.close();


        }catch (Exception e){
            LOG.error("{}", e);
        }
    }

    private void startMonitor(){
        executorService.submit(new ProcessStatusMonitor(zookeeperManager, process));
        executorService.submit(new ProcessErrorMonitor(process));
    }

    public void stop(){
        if(process !=null){
            process.destroy();
        }
        zookeeperManager.stop();
    }
}
