package com.bebe.zookeeper.watcher;

import com.bebe.zookeeper.cluster.ConfigManager;
import com.bebe.zookeeper.common.Configuration;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.bebe.zookeeper.process.Processor;

import java.util.List;
import java.util.Objects;

public class ProcessorsWatcher extends NodesWatcher{
    protected Logger log = LoggerFactory.getLogger(getClass());
    private static final String NODE_NAME = Configuration.getAgentName();
    private Processor processor;
    private ConfigManager configManager;


    public ProcessorsWatcher(ZooKeeper zk, Processor processor, ConfigManager configManager){
        super(zk);
        this.configManager = configManager;
        this.processor = processor;
    }

    @Override
    protected void run(final List<String> children){
        super.run(children);
        synchronized (childrenSet){
            log.info("\t=== Alive processor[{}]:{} ===", childrenSet.size(), Objects.toString(childrenSet));
            log.info("\t");

            int max = configManager.getMaxProcessors();
            if(childrenSet.size()> max){
                if(childrenSet.contains(NODE_NAME)) {
                    log.warn("\t=== Alive processors reach maximum:{} ===", max);
                    processor.stop();
                }
            }else if(childrenSet.size()<max){
                if(!childrenSet.contains(NODE_NAME)) {
                    processor.start();
                }
            }
        }
    }
}
