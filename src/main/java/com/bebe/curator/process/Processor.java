package com.bebe.curator.process;


import com.bebe.common.Configuration;
import com.bebe.common.Constants;
import com.bebe.common.Sleep;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.cluster.ConfigManager;
import com.bebe.curator.cluster.CuratorClientManager;
import com.bebe.curator.cluster.Lock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Processor extends Lock {
    private static final String LOCK_PATH = String.format("%s/process_lock", Cluster.CLUSTER_NODE_PATH);
    public static final String NODE_PATH = String.format("%s/%s", Cluster.PROCESS_NODE_PATH, Constants.AGENT_NAME);
    private static final String PROCESS_PID_FILE = "process.pid";
    private static final int MAX_RETRIES = Configuration.getRetries();

    private Process process;
    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    private ConfigManager configManager;
    private CuratorFramework client;
    private AtomicInteger retries = new AtomicInteger(0);


    public Processor(ConfigManager configManager){
        super(LOCK_PATH);
        this.configManager = configManager;
    }

    @Override
    protected CuratorFramework getClient() {
        client = CuratorClientManager.start();
        return client;
    }

    @Override
    protected void before() {
        stop();
    }

    @Override
    protected void process() {
        try {
            List<String> processes = client.getChildren().forPath(Cluster.PROCESS_NODE_PATH);
            int max = configManager.getMaxProcessors();
            if (processes.size() < max) {
                if (!processes.contains(Constants.AGENT_NAME)) {
                    register();
                }
            } else if (processes.size() > max) {
                if (processes.contains(Constants.AGENT_NAME)) {
                    log.info("\t=== Alive processors reach maximum:{} ===", max);
                    stop();
                }
            }
        }catch (Exception e){
            log.error("\t=== fail to retrieve children:{} ===", e);
            stop();
        }
    }

    private void register(){
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(NODE_PATH);
            runProcess();
        }catch (KeeperException.NodeExistsException e){
            int retry = retries.incrementAndGet();
            if(retry<MAX_RETRIES){
                log.error("\t=== Retry to register process:{}. ===", NODE_PATH);
                register();
            }else{
                log.error("\t=== Duplicated Process:{} registered. ===", NODE_PATH);
                client.close();
            }
        }catch (Exception e){
            log.error("\t=== register:{} ===", e);
            client.close();
        }
    }

    private void runProcess(){
        if(configManager == null){
            log.info("\t=== configManager not ready. ===");
            Sleep.start();
            runProcess();
            return;
        }
        String command = configManager.getCommand();
        try {
            log.info("\t=== exec command:{} ===", command);
            process = Runtime.getRuntime().exec(command);
            outputProcessID();
            startMonitor();
        }catch (IOException e){
            log.error("\t=== Fail to start process:{}. ===", e);
           client.close();
        }
    }

    private void outputProcessID(){
        try {
            Field pidField = process.getClass().getDeclaredField("pid");
            pidField.setAccessible(true);
            Long pid = pidField.getLong(process);
            pidField.setAccessible(false);
            log.info("\t=== process id:{} ===", pid);

            FileWriter writer = new FileWriter(PROCESS_PID_FILE);
            writer.write(String.valueOf(pid));
            writer.close();

        }catch (Exception e){
            log.error("\t=== outputProcessID:{} ===", e);
            stop();
        }
    }

    private void startMonitor(){
        executorService.submit(new ProcessStatusMonitor(client, process));
        executorService.submit(new ProcessErrorMonitor(process));
    }

    public void stop(){
        log.info("\t=== stop. ===");
        if(process !=null){
            process.destroy();
        }
        if(client!=null) {
            client.close();
        }
    }
}
