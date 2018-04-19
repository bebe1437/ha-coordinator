package com.bebe.curator.cache;

import com.bebe.common.Constants;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.cluster.ConfigManager;
import com.bebe.curator.process.Processor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ProcessCache extends AbstractChildrenCache{
    private static final String LOCK_PATH = String.format("%s/check_lock", Cluster.CLUSTER_NODE_PATH);
    private Processor processor;
    private ConfigManager configManager;

    public ProcessCache(CuratorFramework client, Processor processor, ConfigManager configManager){
        super(client, Cluster.PROCESS_NODE_PATH);
        this.processor = processor;
        this.configManager = configManager;
    }

    @Override
    protected void process() {
        run(children);
    }

    private void run(Set<String> children){
        synchronized (children){
            log.info("\t=== Alive processors[{}]:{} ===", children.size(), Objects.toString(children));

            int max = configManager.getMaxProcessors();
            if(children.size()> max){
                if(children.contains(Processor.NODE_PATH)) {
                    log.warn("\t=== Alive processors reach maximum:{} ===", max);
                    processor.stop();
                }
            }else if(children.size()<max){
                if(!children.contains(Processor.NODE_PATH)) {
                    processor.start();
                }
            }
        }
    }

    public void recheck(){
        log.info("\t=== start locking... ===");
        InterProcessMutex lock = new InterProcessMutex(client, LOCK_PATH);
        try {
            if(lock.acquire(Constants.BUFFER_TIME, TimeUnit.MICROSECONDS)){
                Set<String> children = getChildren();
                run(children);
            }
        }catch (Exception e){
            log.error("\t=== fail to do lock:{} ===", e);
            client.close();
        }finally {
            try {
                lock.release();
                log.info("\t=== release lock ===");
            }catch (Exception e){
                log.debug("\t=== release lock:{} ===", e);
            }
        }
    }
}
