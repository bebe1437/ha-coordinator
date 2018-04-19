package com.bebe.zookeeper.watcher;

import com.bebe.zookeeper.cluster.ConfigManager;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigWatcher implements Watcher{
    private static final Logger LOG = LoggerFactory.getLogger(ConfigWatcher.class);
    private ZooKeeper zk;
    private ConfigManager configManager;

    public ConfigWatcher(ConfigManager configManager, ZooKeeper zk){
        this.configManager = configManager;
        this.zk = zk;
    }

    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            check(watchedEvent.getPath());
        }
    }

    void check(String path) {
        zk.getData(path, this, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                switch (KeeperException.Code.get(rc)){
                    case OK:
                        String config = new String(data);
                        LOG.info("\t=== config changed:{}  ===", config);
                        configManager.setUp(config);
                        break;
                    case CONNECTIONLOSS:
                        check(path);
                        break;

                }
            }
        }, null);
    }
}
