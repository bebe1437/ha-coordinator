package com.bebe.curator.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ProcessErrorMonitor implements Runnable{
    private static final  Logger LOG = LoggerFactory.getLogger(ProcessErrorMonitor.class);

    private Process process;

    public ProcessErrorMonitor(Process process){
        this.process = process;
    }

    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                LOG.error(line);
            }
            reader.close();
        }catch (IOException e) {
            LOG.error("\t=== ProcessErrorMonitor:{} ===", e);
        }
    }
}
