package com.bebe.curator.cluster;

import com.bebe.common.ClusterConfig;
import com.bebe.curator.cache.ConfCache;
import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public class ConfigManager extends Lock {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigManager.class);

    private Cluster cluster;
    private CuratorFramework client;

    private ClusterConfig clusterConfig;
    private Gson gson;
    private ConfCache confCache;
    private Charset charset;
    private String confNodePath;

    public ConfigManager(Cluster cluster){
        super(cluster.getBufferTime(), String.format("%s/conf_lock", cluster.getClusterNodePath()));
        LOG.info("createConfigManager");

        this.cluster = cluster;
        this.client = cluster.getClient();
        confNodePath = String.format("%s/conf", cluster.getClusterNodePath());
        gson = new Gson();
        confCache = new ConfCache(client, this);
        charset = Charset.forName("UTF-8");

    }

    public String getConfNodePath(){
        return confNodePath;
    }

    @Override
    protected CuratorFramework getClient() {
        return client;
    }

    public void init(){
        super.start();
    }

    public void stop(){
        confCache.stop();
    }

    public synchronized int getMaxProcessors(){
        return clusterConfig.getMaxProcessors();
    }

    public synchronized String getCommand(){
        return clusterConfig.getCommand();
    }

    public synchronized void setUp(String configuration){
        LOG.info("\t=== setUp config:{} ===", configuration);
        ClusterConfig tmp = null;
        try {
            tmp = gson.fromJson(configuration, ClusterConfig.class);
        }catch (Exception e){
            LOG.error("\t=== fail to reset config[{}]:{} ===", configuration, e);
            return;
        }
        if(clusterConfig == null){
            clusterConfig = new ClusterConfig()
            .setCommand(tmp.getCommand())
            .setMaxProcessors(tmp.getMaxProcessors());
            return;
        }

        if(!tmp.getCommand().equals(clusterConfig.getCommand())){
            clusterConfig.setCommand(tmp.getCommand());
            cluster.startProcess();
        }

        if(tmp.getMaxProcessors()!=clusterConfig.getMaxProcessors()){
            clusterConfig.setMaxProcessors(tmp.getMaxProcessors());
            cluster.recheckMaximumProcessors();
        }
    }

    @Override
    protected void process() {
        try {
            if (client.checkExists().creatingParentsIfNeeded().forPath(confNodePath) == null) {
                clusterConfig = new ClusterConfig()
                        .setCommand(cluster.getCommand())
                        .setMaxProcessors(cluster.getMaxProcessors());
                String json = gson.toJson(clusterConfig);
                LOG.info("\t=== upload config:{} ===", json);
                client.create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(confNodePath, json.getBytes(charset));
            }else{
                byte[] data = client.getData().forPath(confNodePath);
                setUp(new String(data, charset));
            }
            confCache.start();
        }catch (KeeperException.NodeExistsException e){
        }catch (Exception e){
            LOG.error("\t=== check conf node:{} ===", e);
            cluster.shutdown("ConfigManager-process-Exception");
        }
    }
}
