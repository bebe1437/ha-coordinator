package com.bebe.curator;

import com.bebe.common.Config;
import com.bebe.common.Sleep;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.cluster.ClusterFactory;
import com.bebe.curator.process.Processor;
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
import org.mockito.Mockito;

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

        ClusterFactory.Builder builder = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL);
        //Cluster cluster = builder.build();
        Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(cluster.getConf()).thenReturn(
                new Config().setCommand(builder.getCommand())
                .setKill(builder.getKill())
                .setMaxProcessors(builder.getMaxProcessors())
        );
        Mockito.when(cluster.getBuilder()).thenReturn(builder);
        Mockito.when(cluster.getProcessNodePath()).thenReturn(String.format("/%s/processes", CLUSTER_NAME));
        Mockito.when(cluster.getZKHost()).thenReturn(builder.getZkHost());
        Mockito.when(cluster.getSessionTimeout()).thenReturn(builder.getSessionTimeout());
        Mockito.when(cluster.getAgentName()).thenReturn(builder.getAgentName());
        Mockito.when(cluster.getBufferTime()).thenReturn(builder.getBufferTime());
        Mockito.when(cluster.getMaxRetries()).thenReturn(builder.getMaxRetries());

        Processor processor = new Processor(cluster);

        try {
            client.create().creatingParentsIfNeeded().forPath(cluster.getProcessNodePath());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        System.out.println("Start");
        processor.start(builder.getCommand());
        Sleep.start();
        Sleep.start();

        List<String> processes = null;
        try{
            processes = client.getChildren().forPath(cluster.getProcessNodePath());
            Assert.assertEquals(1, processes.size());
            System.out.println(processes);
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        System.out.println("kill");
        try {
            KillSession.kill(processor.getClient().getZookeeperClient().getZooKeeper(), server.getConnectString());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        Sleep.start();
        Sleep.start();

        try{
            processes = client.getChildren().forPath(cluster.getProcessNodePath());
            System.out.println(processes);
            Assert.assertEquals(1, processes.size());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        processor.stop("test");
        CloseableUtils.closeQuietly(server);
    }
}
