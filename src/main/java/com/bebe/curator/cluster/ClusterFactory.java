package com.bebe.curator.cluster;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterFactory.class);

    public static Builder builder(){
        return new Builder();
    }

    public static class Builder{
        private String zkHost;
        private int sessionTimeout;
        private String clusterName;
        private String agentName;
        private int maxRetries = 3;
        private int maxProcessors;
        private String command;
        private long bufferTime = 3000l;

        public Cluster build(){
            LOG.info("\t=== zkHost:{} ===", zkHost);
            LOG.info("\t=== sessionTimeout:{} ===", sessionTimeout);
            LOG.info("\t=== clusterName:{} ===", clusterName);
            LOG.info("\t=== agentName:{} ===", agentName);
            LOG.info("\t=== maxRetries:{} ===", maxRetries);
            LOG.info("\t=== maxProcessors:{} ===", maxProcessors);
            LOG.info("\t=== bufferTime:{} ===", bufferTime);
            LOG.info("\t=== command:{} ===", command);

            RetryPolicy retryPolicy = new ExponentialBackoffRetry((int)bufferTime, maxRetries);
            CuratorFramework client = CuratorFrameworkFactory
                    .builder()
                    .connectString(zkHost)
                    .retryPolicy(retryPolicy)
                    .sessionTimeoutMs(sessionTimeout)
                    .build();
            client.getCuratorListenable().addListener(new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                    LOG.info("{}", curatorEvent.toString());
                }
            });
            client.start();
            return new Cluster(this, client);
        }

        public long getBufferTime() {
            return bufferTime;
        }

        public Builder setBufferTime(long bufferTime) {
            this.bufferTime = bufferTime;
            return this;
        }

        public String getCommand() {
            return command;
        }

        public Builder setCommand(String command) {
            this.command = command;
            return this;
        }

        public int getMaxProcessors() {
            return maxProcessors;
        }

        public Builder setMaxProcessors(int maxProcessors) {
            this.maxProcessors = maxProcessors;
            return this;
        }

        public String getZkHost() {
            return zkHost;
        }

        public Builder setZkHost(String zkHost) {
            this.zkHost = zkHost;
            return this;
        }

        public int getSessionTimeout() {
            return sessionTimeout;
        }

        public Builder setSessionTimeout(int sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
            return this;
        }

        public String getClusterName() {
            return clusterName;
        }

        public Builder setClusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public String getAgentName() {
            return agentName;
        }

        public Builder setAgentName(String agentName) {
            this.agentName = agentName;
            return this;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public Builder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }
    }
}
