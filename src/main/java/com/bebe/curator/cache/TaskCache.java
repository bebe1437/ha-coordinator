package com.bebe.curator.cache;

import com.bebe.common.Task;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.process.Processor;
import com.google.gson.Gson;

public class TaskCache extends AbstractNodeCache{

    private Cluster cluster;
    private Gson gson;
    private Processor processor;

    public TaskCache(Cluster cluster, Processor processor){
        super(cluster.getClient(), cluster.getInfoTaskPath());
        gson = new Gson();
        this.cluster = cluster;
        this.processor = processor;
    }

    @Override
    public void process(byte[] data) {
        String infoStr = new String(data, charset);
        log.info("\t=== task:{} ===", infoStr);
        if(infoStr.isEmpty()){
            return;
        }
        Task task = null;
        try {
            task = gson.fromJson(infoStr, Task.class);
        }catch (Exception e){
            log.error("\t=== fail to convert json:{} ===", infoStr);
            return;
        }

        if(task.getRunningProcessIDs().contains(cluster.getAgentName())){
            Long processID = processor.getProcessID();
            if(processID==null){
                processor.restart("TaskCache", task.getCommand());
            }else if(!cluster.getConf().getCommand().equals(task.getCommand())){
                cluster.getConf().setCommand(task.getCommand());
                processor.restart("TaskCache", task.getCommand());
            }
        }else{
            processor.stop("TaskCache");
        }

    }

    @Override
    public void remove() {
        log.warn("\t=== {} was removed. ===", cluster.getInfoTaskPath());
    }
}
