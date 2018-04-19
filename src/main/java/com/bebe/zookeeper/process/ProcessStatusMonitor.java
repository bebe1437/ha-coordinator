package com.bebe.zookeeper.process;

import com.bebe.zookeeper.cluster.ZookeeperManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class ProcessStatusMonitor implements Callable<Integer>{
    private static final  Logger LOG = LoggerFactory.getLogger(ProcessStatusMonitor.class);

    private Process process;
    private ZookeeperManager zkManager;


    public ProcessStatusMonitor(ZookeeperManager zkManager , Process process){
        this.process = process;
        this.zkManager = zkManager;
    }

    public Integer call() {
        while(true){
            try{
                int exitValue = process.exitValue();
                LOG.error("\t=== Processor exit:{} ===", exitValue);
                zkManager.stop();
                return exitValue;
            }catch (Exception e){
                //LOG.info("\t=== Processor is still running. ===");
            }
        }
    }
}
