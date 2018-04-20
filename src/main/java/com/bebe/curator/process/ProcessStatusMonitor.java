package com.bebe.curator.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class ProcessStatusMonitor implements Callable<Integer>{
    private static final  Logger LOG = LoggerFactory.getLogger(ProcessStatusMonitor.class);

    private Process process;
    private Processor processor;


    public ProcessStatusMonitor(Processor processor , Process process){
        this.process = process;
        this.processor = processor;
    }

    public Integer call() {
        while(true){
            try{
                int exitValue = process.exitValue();
                LOG.error("\t=== Processor exit:{} ===", exitValue);
                processor.restart("ProcessStatusMonitor");
                return exitValue;
            }catch (Exception e){
                //LOG.info("\t=== Processor is still running. ===");
            }
        }
    }
}
