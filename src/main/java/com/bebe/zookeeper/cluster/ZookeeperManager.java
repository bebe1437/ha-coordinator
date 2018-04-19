package com.bebe.zookeeper.cluster;

import com.bebe.zookeeper.common.Sleep;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ZookeeperManager {
    private Logger log = LoggerFactory.getLogger(getClass());
    private ZooKeeper zk;
    private String zkHost;
    private int sessionTimeout = 15000;

    public ZookeeperManager(String zkHost, int sessionTimeout){
        this.zkHost = zkHost;
        this.sessionTimeout = sessionTimeout;
    }

    public ZooKeeper start(){
        try {
            zk = new ZooKeeper(zkHost, sessionTimeout, new Watcher() {

                public void process(WatchedEvent watchedEvent) {
//                    log.info("\t=== event:{} ==="
//                            , watchedEvent.getType().name());
                }
            });
            Sleep.start();
            return zk;
        }catch (IOException e){
            log.error("\t=== Fail to start zookeeper:{} ===", e);
        }
        return null;
    }

    public void stop(){
        try {
            if(zk !=null) {
                zk.close();
            }
        }catch (InterruptedException e){
            log.error("\t=== Fail to stop zookeeper session:{}. ===", e);
        }
    }

}
