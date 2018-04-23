package com.bebe.common;

import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.cluster.ClusterFactory;

public class Main {

    public static void main(String[] args){
        Cluster cluster = ClusterFactory.builder()
                .setZkHost(Configuration.getZKHost())
                .setSessionTimeout(Configuration.getSessionTimeout())
                .setMaxRetries(Configuration.getRetries())
                .setMaxProcessors(Configuration.getMaximunProcessors())
                .setClusterName(Configuration.getClusterName())
                .setAgentName(Configuration.getAgentName())
                .setCommand(Configuration.getCommand())
                .build();

        cluster.start();

        while (!cluster.isShutdown());
    }
}
