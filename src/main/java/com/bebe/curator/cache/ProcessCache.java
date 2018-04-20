package com.bebe.curator.cache;

import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.cluster.ConfigManager;
import com.bebe.curator.process.Processor;
import org.apache.curator.framework.CuratorFramework;

import java.util.*;

//use watch
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
            String nodeName = processor.getCreatedPath();
            if(nodeName == null){
                return;
            }
            nodeName = nodeName.replaceAll(Cluster.PROCESS_NODE_PATH+"/","");
            if(children.size()> max){
                if(children.contains(nodeName)) {
                    log.warn("\t=== Alive processors reach maximum:{} ===", max);
                    processor.stop("ProcessCache-reach maximum");
                }
            }else if(children.size()<max){
                if(!children.contains(nodeName)) {
                    processor.restart("ProcessCache");
                }
            }
        }
    }

    public void recheck(){
        try {
            List<String> childData = client.getChildren().forPath(Cluster.PROCESS_NODE_PATH);

            Set<String> tmp = new HashSet<String>();
            for (String path : childData) {
                tmp.add(path);
                if (children.contains(path)) {
                    continue;
                }
                children.add(path);
                log.info("\t=== add:{}  ===", path);
            }

            for (Iterator<String> it = children.iterator(); it.hasNext(); ) {
                String path = it.next();
                if (!tmp.contains(path)) {
                    it.remove();
                    log.info("\t=== remove:{} ===", path);
                }
            }

            run(children);
        }catch (Exception e){
            log.error("\t=== fail to recheck:{} ===", e);
            client.close();
        }
    }
}
