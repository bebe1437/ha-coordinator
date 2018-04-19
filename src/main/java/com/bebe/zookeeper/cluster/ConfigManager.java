package com.bebe.zookeeper.cluster;

import com.bebe.zookeeper.common.ClusterConfig;
import com.bebe.zookeeper.common.Configuration;
import com.bebe.zookeeper.watcher.ConfigWatcher;
import com.google.gson.Gson;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class ConfigManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigManager.class);
    private static final Gson GSON = new Gson();

    public static final String CONFIG_NODE_PATH = String.format("%s/conf", Cluster.CLUSTER_NODE_PATH);

    private Random random;
    private ZooKeeper zk;
    private ClusterConfig clusterConfig;
    private Cluster cluster;

    public ConfigManager(Cluster cluster, ZooKeeper zk){
        this.cluster = cluster;
        this.zk = zk;
        clusterConfig = new ClusterConfig();
        random = new Random();
    }

    public void initial(){
        zk.getData(CONFIG_NODE_PATH, new ConfigWatcher(this, zk), new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                switch (KeeperException.Code.get(rc)){
                    case OK:
                        String config = new String(data);
                        LOG.info("\t=== getData:{} ===", config);
                        setUp(config);
                        break;
                    case NONODE:
                        createConfigNode(path);
                        break;
                    case CONNECTIONLOSS:
                        initial();
                        break;
                    default:
                        LOG.error("\t=== Fail to retrieve config [{}]:{} ===", rc, KeeperException.create(KeeperException.Code.get(rc), path));
                        cluster.shutdown();
                }
            }
        }, null);
    }

    private void createConfigNode(final String path){
        zk.create(path
                , Integer.toHexString(random.nextInt()).getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE
                , CreateMode.PERSISTENT
                , new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        switch (KeeperException.Code.get(rc)){
                            case OK:
                                setData(path);
                            case NODEEXISTS:
                                initial();
                                break;
                            case CONNECTIONLOSS:
                                createConfigNode(path);
                                break;
                            default:
                                LOG.error("\t=== Fail to register config [{}]:{} ===", rc, KeeperException.create(KeeperException.Code.get(rc), path));
                                cluster.shutdown();
                        }
                    }
                }
                , null);
    }

    private synchronized void setData(String path){
        try {
            clusterConfig = new ClusterConfig()
            .setCommand(Configuration.getCommand())
            .setMaxProcessors(Configuration.getMaximunProcessors());
            LOG.info("\t=== upload config:{} ===", GSON.toJson(clusterConfig));
            zk.setData(path, (GSON.toJson(clusterConfig)).getBytes(), -1);
        }catch (Exception e){
            LOG.error("\t=== Fail to update config:{} ===", e);
            cluster.shutdown();
        }
    }


    public synchronized int getMaxProcessors(){
        return clusterConfig.getMaxProcessors();
    }

    public synchronized String getCommand(){
        return clusterConfig.getCommand();
    }

    public synchronized void setUp(String configuration){
        LOG.info("\t=== setUp config:{} ===", configuration);
        ClusterConfig tmp = GSON.fromJson(configuration, ClusterConfig.class);
        if(!tmp.getCommand().equals(clusterConfig.getCommand())){
            clusterConfig.setCommand(tmp.getCommand());
            cluster.startProcess();
        }

        if(tmp.getMaxProcessors()!=clusterConfig.getMaxProcessors()){
            clusterConfig.setMaxProcessors(tmp.getMaxProcessors());
            cluster.checkAliveProcessors();
        }
    }
}
