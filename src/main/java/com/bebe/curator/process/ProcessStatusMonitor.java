package com.bebe.curator.process;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class ProcessStatusMonitor implements Callable<Integer>{
    private static final  Logger LOG = LoggerFactory.getLogger(ProcessStatusMonitor.class);

    private Process process;
    private CuratorFramework client;


    public ProcessStatusMonitor(CuratorFramework client , Process process){
        this.process = process;
        this.client = client;
    }

    public Integer call() {
        while(true){
            try{
                int exitValue = process.exitValue();
                LOG.error("\t=== Processor exit:{} ===", exitValue);
                client.close();
                return exitValue;
            }catch (Exception e){
                //LOG.info("\t=== Processor is still running. ===");
            }
        }
    }
}
