package com.bebe.curator;

import com.bebe.common.ClusterConfig;
import com.bebe.common.Sleep;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.cluster.ClusterFactory;
import com.bebe.curator.cluster.ConfigManager;
import com.bebe.curator.process.Processor;
import com.google.gson.Gson;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ProcessorTest {
    private static final String CLUSTER_NAME = "ClusterTest";
    private static final int SESSION_TIMEOUT = 15000;
    private static final int RETRIES = 3;
    private static final String KILL = "./kill.sh";

    private CuratorFramework client;
    private TestingServer server;

    @Before
    public void setUp(){
        try {
            server = new TestingServer(-1);
        }catch (Exception e){
            Assert.assertNull(e);
        }

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(SESSION_TIMEOUT, RETRIES);
        client = CuratorFrameworkFactory
                .builder()
                .connectString(server.getConnectString())
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .build();
        client.start();
    }

    @After
    public void destroy(){
        client.close();
    }


    /**
     * maxProcessors: 1
     * start one agents,
     * check if there are one processor running.
     * */
    @Test
    public void test01_sessionExpired(){
        int maxProcessors = 1;
        String agentName = "agentTest01";

        Cluster cluster = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL)
                .build();

        ClusterConfig clusterConfig = new ClusterConfig()
                .setMaxProcessors(maxProcessors)
                .setCommand("top")
                .setKill(KILL);
        ConfigManager configManager = new ConfigManager(cluster);
        configManager.setUp(new Gson().toJson(clusterConfig));
        Processor processor = new Processor(cluster, configManager);

        try {
            client.create().creatingParentsIfNeeded().forPath(cluster.getProcessNodePath());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        processor.start();
        Sleep.start();

        List<String> processes = null;
        String process = null;
        try{
            processes = client.getChildren().forPath(cluster.getProcessNodePath());
            process = processes.get(0);
            System.out.println(processes);
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        try {
            KillSession.kill(processor.getClient().getZookeeperClient().getZooKeeper(), server.getConnectString());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        Sleep.start();
        Sleep.start();
        Sleep.start();
        Sleep.start();

        try{
            processes = client.getChildren().forPath(cluster.getProcessNodePath());
            System.out.println(processes);
            Assert.assertEquals(1, processes.size());
            Assert.assertTrue(!process.equals(processes.get(0)));
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        processor.stop("test");
        CloseableUtils.closeQuietly(server);
    }
}
