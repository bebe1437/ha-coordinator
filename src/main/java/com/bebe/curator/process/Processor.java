package com.bebe.curator.process;


import com.bebe.common.Sleep;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.cluster.StateListener;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class Processor implements Observer{
    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);
    private static final String PROCESS_PID_FILE = "process.pid";

    private Cluster cluster;
    private Process process;
    private ExecutorService executorService = Executors.newFixedThreadPool(10);
    private CuratorFramework client;
    private AtomicInteger retries = new AtomicInteger(0);
    private AtomicInteger seq = new AtomicInteger(0);

    private Semaphore restartLock = new Semaphore(1);
    private String nodePath;
    private Long processID;

    public Processor(Cluster cluster){
        this.cluster = cluster;
        nodePath = String.format("%s/%s", cluster.getProcessNodePath(), cluster.getAgentName());
    }

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
            client.getConnectionStateListenable().addListener(
                    new StateListener("proccess-" + seq.incrementAndGet(), cluster, this));
            client.start();
            this.client = client;
            return client;
        }
    }

    public synchronized Long getProcessID(){
        return processID;
    }

    public void start(String command){
        try {
            getClient().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(nodePath);
            runProcess(command);
        }catch (KeeperException.NodeExistsException e){
            int retry = retries.incrementAndGet();
            if(retry<cluster.getMaxRetries()){
                LOG.error("\t=== Retry to register process:{}. ===", nodePath);
                start(command);
            }else{
                LOG.error("\t=== Duplicated Process:{} registered. ===", nodePath);
                client.close();
            }
        }catch (Exception e){
            LOG.error("\t=== start:{} ===", e);
            stop("start");
        }finally {
            //cluster.startProcessCache();
        }
    }

    private synchronized void runProcess(String command){
        try {
            LOG.info("\t=== exec command:{} ===", command);
            if(command==null || command.isEmpty()){
                LOG.error("\t=== Invalid command:{} ===", command);
                client.close();
                return;
            }
            process = Runtime.getRuntime().exec(command);
            outputProcessID();
            startMonitor();
        }catch (IOException e){
            LOG.error("\t=== Fail to start process:{}. ===", e);
           client.close();
        }
    }

    private synchronized void outputProcessID(){
        try {
            Field pidField = process.getClass().getDeclaredField("pid");
            pidField.setAccessible(true);
            processID = pidField.getLong(process);
            pidField.setAccessible(false);
            LOG.info("\t=== process id:{} ===", processID);

            FileWriter writer = new FileWriter(PROCESS_PID_FILE);
            writer.write(String.valueOf(processID));
            writer.close();

        }catch (Exception e){
            LOG.error("\t=== outputProcessID:{} ===", e);
            stop("outputProcessID");
        }
    }

    private synchronized void startMonitor(){
        executorService.submit(new ProcessStatusMonitor(this, process));
        executorService.submit(new ProcessErrorMonitor(process));
    }

    public synchronized void stop(String who){
        LOG.info("\t=== {} stop ===", who);
        destroy();
        if(client!=null) {
            try {
                if(processID!=null) {
                    client.delete().guaranteed().forPath(nodePath);
                }
            }catch (KeeperException.NoNodeException e){
            }catch (Exception e){
                LOG.error("\t=== fail to delete:{} ===", e);
                client.close();
            }
        }

        if(process!=null) {
            LOG.info("\t=== kill process. ===");
            processID = null;
            process.destroy();
            process = null;
        }
    }

    private synchronized void destroy(){
        if(processID!=null) {
            LOG.info("\t=== kill children process:{}  ===", processID);
            Process killer = null;
            try {
                String command = String.format("%s %s", cluster.getConf().getKill(), processID);
                LOG.debug("{}", command);
                killer = Runtime.getRuntime().exec(command);
                executorService.submit(new ProcessInfoMonitor(killer));
                executorService.submit(new ProcessErrorMonitor(killer));
            } catch (Exception e) {
                LOG.error("\t=== fail to destroy children processes:{} ===", e);
            }

            //make sure killer finish.
            if(killer!=null){
                while (true) {
                    try {
                        killer.exitValue();
                        break;
                    }catch (Exception e){}
                }
            }
        }
    }

    public void restart(String who, String command){
        try {
            restartLock.acquire();
            //sleep for a while in case restart too often
            Sleep.start();
            stop(who + "-restart");
            start(command);
        }catch (Exception e){
            LOG.error("\t=== restart:{} ===", e);
            restart(who, command);
        }finally{
            restartLock.release();
        }
    }

    @Override
    public void update(Observable o, Object arg) {

    }
}
