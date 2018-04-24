package com.bebe.curator.process;


import com.bebe.common.Sleep;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.cluster.ConfigManager;
import com.bebe.curator.cluster.Lock;
import com.bebe.curator.cluster.StateListener;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class Processor extends Lock {
    private static final String PROCESS_PID_FILE = "process.pid";

    private Cluster cluster;
    private Process process;
    private ExecutorService executorService = Executors.newFixedThreadPool(4);
    private ConfigManager configManager;
    private CuratorFramework client;
    private AtomicInteger retries = new AtomicInteger(0);
    private AtomicInteger seq = new AtomicInteger(0);

    private String createdPath;
    private Semaphore restartLock = new Semaphore(1);
    private String nodePath;
    private Long processID;

    public Processor(Cluster cluster, ConfigManager configManager){
        super(cluster.getBufferTime(), String.format("%s/process_lock", cluster.getClusterNodePath()));
        log.info("createProcessor");

        this.cluster = cluster;
        this.configManager = configManager;
        nodePath = String.format("%s/%s", cluster.getProcessNodePath(), cluster.getAgentName());
    }

    @Override
    public CuratorFramework getClient() {
        if(client!=null && client.getZookeeperClient().isConnected()){
            return client;
        }else {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry((int)cluster.getBufferTime(), cluster.getMaxRetries());
            CuratorFramework client = CuratorFrameworkFactory
                    .builder()
                    .connectString(cluster.getZKHost())
                    .retryPolicy(retryPolicy)
                    .sessionTimeoutMs(cluster.getSessionTimeout())
                    .build();
            client.getConnectionStateListenable().addListener(new StateListener("proccess-" + seq.incrementAndGet(), this));
            client.start();
            this.client = client;
            return client;
        }
    }

    @Override
    protected void process() {
        try {
            List<String> processes = client.getChildren().forPath(cluster.getProcessNodePath());
            int max = configManager.getMaxProcessors();
            if (processes.size() < max) {
                if (!processes.contains(cluster.getAgentName())) {
                    register();
                }
            } else if (processes.size() > max) {
                if (processes.contains(cluster.getAgentName())) {
                    log.info("\t=== Alive processors reach maximum:{} ===", max);
                    stop("process-reach maximum");
                }
            }
        }catch (Exception e){
            log.error("\t=== fail to retrieve children:{} ===", e);
            stop("process-exception");
        }
    }

    public synchronized String getCreatedPath(){
        return createdPath;
    }

    private synchronized void register(){
        try {
            createdPath = client.create()
                    .creatingParentsIfNeeded()
                    .withProtection()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(nodePath);
            runProcess();
        }catch (KeeperException.NodeExistsException e){
            int retry = retries.incrementAndGet();
            if(retry<cluster.getMaxRetries()){
                log.error("\t=== Retry to register process:{}. ===", nodePath);
                register();
            }else{
                log.error("\t=== Duplicated Process:{} registered. ===", nodePath);
                client.close();
            }
        }catch (Exception e){
            log.error("\t=== register:{} ===", e);
            client.close();
        }finally {
            //cluster.startProcessCache();
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

    private synchronized void outputProcessID(){
        try {
            Field pidField = process.getClass().getDeclaredField("pid");
            pidField.setAccessible(true);
            processID = pidField.getLong(process);
            pidField.setAccessible(false);
            log.info("\t=== process id:{} ===", processID);

            FileWriter writer = new FileWriter(PROCESS_PID_FILE);
            writer.write(String.valueOf(processID));
            writer.close();

        }catch (Exception e){
            log.error("\t=== outputProcessID:{} ===", e);
            stop("outputProcessID");
        }
    }

    private void startMonitor(){
        executorService.submit(new ProcessStatusMonitor(this, process));
        executorService.submit(new ProcessErrorMonitor(process));
    }

    public void stop(String who){
        log.info("\t=== {} stop ===", who);
        destroy();
        if(client!=null) {
            try {
                if(createdPath!=null) {
                    synchronized (createdPath) {
                        client.delete().guaranteed().forPath(createdPath);
                    }
                }
            }catch (KeeperException.NoNodeException e){
            }catch (Exception e){
                log.error("\t=== fail to delete:{} ===", e);
                client.close();
            }
        }
    }

    private synchronized void destroy(){
        log.info("\t=== kill children process:{}  ===", processID);
        if(processID!=null) {
            try {
                Process process = Runtime.getRuntime().exec(String.format("%s %s", configManager.getKill(), processID));
                executorService.submit(new ProcessErrorMonitor(process));
            } catch (Exception e) {
                log.error("\t=== fail to destroy children processes:{} ===", e);
            }
        }
        if(process!=null) {
            process.destroy();
        }
    }

    public void restart(String who){
        try {
            restartLock.acquire();
            //sleep for a while in case restart too often
            Sleep.start();
            stop(who + "-restart");
            start();
        }catch (Exception e){
            log.error("\t=== restart:{} ===", e);
            restart(who);
        }finally{
            restartLock.release();
        }
    }
}
