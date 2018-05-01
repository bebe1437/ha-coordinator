package com.bebe.curator.node;

import com.bebe.common.Config;
import com.bebe.curator.cache.Cache;
import com.bebe.curator.cache.ConfCache;
import com.bebe.curator.cluster.Cluster;
import com.google.gson.Gson;

public class ConfNode extends AbsractNode{

    private Gson gson;
    private Cluster cluster;
    private Config config;

    public ConfNode(Cluster cluster){
        super(cluster.getConfNodePath(), true, cluster.getClient());
        this.cluster = cluster;
        gson = new Gson();
    }

    @Override
    protected Cache getCache() {
        return new ConfCache(cluster, this);
    }

    @Override
    protected void handleNodeException(Exception e) {
        cluster.shutdown("ConfNode");
    }

    @Override
    protected void nodeExists() {
        //do nothing.
    }

    public synchronized void init() {
        if(exists()){
            try {
                byte[] data = cluster.getClient().getData().forPath(cluster.getConfNodePath());
                update(new String(data, charset));
            }catch (Exception e){
                handleException(e);
            }
            return;
        }
        config = new Config();
        config.setCommand(cluster.getBuilder().getCommand())
                .setMaxProcessors(cluster.getBuilder().getMaxProcessors())
                .setKill(cluster.getBuilder().getKill());
        String json = gson.toJson(config);
        log.info("\t=== upload conf:{}  ===", json);
        setData(json.getBytes(charset));
    }

    public synchronized void update(String conf){
        log.info("\t=== get conf:{}  ===", conf);
        if(config == null){
            config = gson.fromJson(conf, Config.class);
            return;
        }
        Config tmp  = gson.fromJson(conf, Config.class);
        if(!tmp.getCommand().equals(config.getCommand())){
            config.setCommand(tmp.getCommand());
            cluster.assignTask(tmp.getCommand());
        }

        if(tmp.getMaxProcessors()!=config.getMaxProcessors()){
            config.setMaxProcessors(tmp.getMaxProcessors());
            if(cluster.isMaster()) {
                cluster.assignTask();
            }
        }

        if(!tmp.getKill().equals(config.getKill())){
            config.setKill(tmp.getKill());
        }
    }

    public synchronized Config getConf(){
        return config;
    }
}
