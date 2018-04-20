package com.bebe.curator.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateListener implements ConnectionStateListener {
    private Logger log = LoggerFactory.getLogger(getClass());
    private String name;

    public StateListener(String name){
        this.name = name;
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        log.info(String.format("%s:%s", name, connectionState));
    }

}
