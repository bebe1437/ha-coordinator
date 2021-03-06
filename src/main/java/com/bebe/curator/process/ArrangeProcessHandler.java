package com.bebe.curator.process;

import com.bebe.common.Constants;
import com.bebe.common.Task;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.node.TaskNode;
import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ArrangeProcessHandler {
    private Logger log = LoggerFactory.getLogger(getClass());
    private Gson gson;
    private CuratorFramework client;
    private Cluster cluster;
    private TaskNode taskNode;
    private Charset charset = Constants.UTF8;

    public ArrangeProcessHandler(Cluster cluster, TaskNode taskNode){
        this.cluster = cluster;
        this.taskNode = taskNode;
        this.client = cluster.getClient();
        gson = new Gson();
    }

    public void process(String command){
        List<String> agents = null;
        List<String> processes = null;
        InterProcessMutex lock = new InterProcessMutex(client, cluster.getProcessLockPath());
        try {
            lock.acquire(cluster.getBufferTime(), TimeUnit.MICROSECONDS);
            get(agents, processes);
        }catch (Exception e){
            log.warn("\t=== fail to acquire lock:{} ===", e);
            return;
        }finally {
            try {
                lock.release();
            }catch (Exception e){
                log.warn("\t=== fail to release lock:{} ===", e);
            }
        }
        try {
            agents = client.getChildren().forPath(cluster.getAgentNodePath());
            processes = client.getChildren().forPath(cluster.getProcessNodePath());
        }catch (Exception e){
            log.error("\t=== arrange-getChildren ===");
            cluster.shutdown("arrange-getChildren");
            return;
        }

        int max = cluster.getConf().getMaxProcessors();
        Task task = new Task().addRunningProcessID(new HashSet<String>(processes));
        if(command!=null){
            task.setCommand(command);
        }

        if(processes.size()<max){
            // call process to start
            for(String agent: agents){
                if(!processes.contains(agent)){
                    task.addRunningProcessID(agent);
                    if(task.getRunningProcessIDs().size()==max){
                        break;
                    }
                }
            }
        }else if(processes.size()>max){
            // call process to stop
            int size = task.getRunningProcessIDs().size();
            while (size>max) {
                String process = task.getRunningProcessIDs().iterator().next();
                task.removeRunningProcessID(process);
                size = task.getRunningProcessIDs().size();
            }
        }

        if(processes.size()!=task.getRunningProcessIDs().size()
                || command!=null){
            String json = gson.toJson(task);
            log.info("\t=== upload task:{} ===", json);
            taskNode.setData(json.getBytes(charset));
        }
    }

    private void get(List<String> agents, List<String> processes){
        try {
            agents = client.getChildren().forPath(cluster.getAgentNodePath());
            processes = client.getChildren().forPath(cluster.getProcessNodePath());
        }catch (Exception e){
            log.error("\t=== arrange-getChildren ===");
            cluster.shutdown("arrange-getChildren");
            return;
        }
    }
}
