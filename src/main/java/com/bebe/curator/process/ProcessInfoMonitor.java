package com.bebe.curator.process;

import com.bebe.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ProcessInfoMonitor implements Runnable{
    private static final  Logger LOG = LoggerFactory.getLogger(ProcessInfoMonitor.class);

    private Process process;

    public ProcessInfoMonitor(Process process){
        this.process = process;
    }

    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), Constants.UTF8));
            String line;
            while (reader.ready() && (line = reader.readLine()) != null) {
                LOG.error(line);
            }
            reader.close();
        }catch (IOException e) {
            LOG.error("\t=== ProcessErrorMonitor:{} ===", e);
        }
    }
}
