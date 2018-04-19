package com.bebe.curator.cache;

import com.bebe.curator.cluster.ConfigManager;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.Charset;

public class ConfCache extends AbstractNodeCache{

    private ConfigManager configManager;

    public ConfCache(CuratorFramework client, ConfigManager configManager){
        super(client, ConfigManager.NODE_PATH);
        this.configManager = configManager;
    }

    @Override
    protected void process(byte[] data) {
        configManager.setUp(new String(data, Charset.forName("UTF-8")));
    }

    @Override
    protected void remove() {
        log.warn("\t=== {} was removed. ===", ConfigManager.NODE_PATH);
    }
}
