package com.bebe.curator;

import com.bebe.common.Sleep;
import com.bebe.curator.cluster.Cluster;
import com.bebe.curator.cluster.ClusterFactory;
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

public class ClusterTest {
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
        Sleep.start();
    }


    /**
     * maxProcessors: 1
     * start one agents,
     * check if there are one processor running.
     * */
    @Test
    public void test01_oneAgent_max1(){
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
        cluster.start();

        Sleep.start();

        List<String> processes = null;
        List<String> agents = null;

        try{
            processes = client.getChildren().forPath(cluster.getProcessNodePath());
            agents = client.getChildren().forPath(cluster.getAgentNodePath());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        Assert.assertEquals(maxProcessors, processes.size());
        Assert.assertEquals(1, agents.size());
        Assert.assertTrue(agents.get(0).endsWith(agentName));
        Assert.assertTrue(processes.get(0).endsWith(agentName));

        cluster.shutdown("Test");

        CloseableUtils.closeQuietly(server);
    }

    /**
     * maxProcessors: 1
     * start two agents,
     * check if there are one processor running.
     * */
    @Test
    public void test02_twoAgent_max1(){
        int maxProcessors = 1;
        String agentName1 = "agentTest01";
        String agentName2 = "agentTest02";

        Cluster cluster1 = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName1)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL)
                .build();

        Cluster cluster2 = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName2)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL)
                .build();

        cluster1.start();
        cluster2.start();

        Sleep.start();

        List<String> processes = null;
        List<String> agents = null;

        try{
            processes = client.getChildren().forPath(cluster1.getProcessNodePath());
            agents = client.getChildren().forPath(cluster1.getAgentNodePath());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        Assert.assertEquals(maxProcessors, processes.size());
        Assert.assertEquals(2, agents.size());

        boolean agent1Found = false;
        boolean agent2Found = false;
        for(String agent: agents){
            if(agent.endsWith(agentName1)){
                agent1Found = true;
            }else if(agent.endsWith(agentName2)){
                agent2Found = true;
            }
        }
        Assert.assertTrue(agent1Found && agent2Found);
        Assert.assertTrue(processes.get(0).endsWith(agentName1) || processes.get(0).endsWith(agentName2));

        cluster1.shutdown("Test");
        cluster2.shutdown("Test");

        CloseableUtils.closeQuietly(server);
    }

    /**
     * maxProcessors: 2
     * start two agents,
     * check if there are two processors running.
     * */
    @Test
    public void test03_twoAgent_max2(){
        int maxProcessors = 2;
        String agentName1 = "agentTest01";
        String agentName2 = "agentTest02";

        Cluster cluster1 = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName1)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL)
                .build();

        Cluster cluster2 = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName2)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL)
                .build();

        cluster1.start();
        cluster2.start();

        Sleep.start();

        List<String> processes = null;
        List<String> agents = null;

        try{
            processes = client.getChildren().forPath(cluster1.getProcessNodePath());
            agents = client.getChildren().forPath(cluster1.getAgentNodePath());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        Assert.assertEquals(maxProcessors, processes.size());
        Assert.assertEquals(2, agents.size());

        boolean agent1Found = false;
        boolean agent2Found = false;
        for(String agent: agents){
            if(agent.endsWith(agentName1)){
                agent1Found = true;
            }else if(agent.endsWith(agentName2)){
                agent2Found = true;
            }
        }
        Assert.assertTrue(agent1Found && agent2Found);

        agent1Found = false;
        agent2Found = false;
        for(String agent: processes){
            if(agent.endsWith(agentName1)){
                agent1Found = true;
            }else if(agent.endsWith(agentName2)){
                agent2Found = true;
            }
        }
        Assert.assertTrue(agent1Found && agent2Found);

        cluster1.shutdown("Test");
        cluster2.shutdown("Test");

        CloseableUtils.closeQuietly(server);
    }

    /**
     * maxProcessors: 1
     * start two agents,
     * shutdown running processor's agent
     * and check if start an new processor.
     * */
    @Test
    public void test04(){
        int maxProcessors = 1;
        String agentName1 = "agentTest01";
        String agentName2 = "agentTest02";

        Cluster cluster1 = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName1)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL)
                .build();

        Cluster cluster2 = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName2)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL)
                .build();

        cluster1.start();
        cluster2.start();

        Sleep.start();

        List<String> processes = null;
        List<String> agents = null;

        try{
            processes = client.getChildren().forPath(cluster1.getProcessNodePath());
            agents = client.getChildren().forPath(cluster1.getAgentNodePath());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }


        Assert.assertEquals(maxProcessors, processes.size());
        Assert.assertEquals(2, agents.size());
        Assert.assertTrue(processes.get(0).endsWith(agentName1) || processes.get(0).endsWith(agentName2));

        //shutdown cluster
        System.out.println("shutdown cluster");
        String processAgent = null;
        if(processes.get(0).endsWith(agentName1)){
            cluster1.shutdown("test");
            processAgent = agentName2;
        }else{
            cluster2.shutdown("test");
            processAgent = agentName1;
        }

        Sleep.start();
        Sleep.start();
        Sleep.start();
        Sleep.start();

        System.out.println("getChildren");
        try{
            processes = client.getChildren().forPath(cluster1.getProcessNodePath());
            agents = client.getChildren().forPath(cluster1.getAgentNodePath());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }


        Assert.assertEquals(maxProcessors, processes.size());
        Assert.assertEquals(1, agents.size());
        Assert.assertTrue(processes.get(0).endsWith(processAgent));
        Assert.assertEquals(processAgent, agents.get(0));


        if(processAgent.endsWith(agentName1)) {
            cluster1.shutdown("Test");
        }else {
            cluster2.shutdown("Test");
        }

        CloseableUtils.closeQuietly(server);
    }

    @Test
    public void test05(){
        int maxProcessors = 1;
        String agentName1 = "agentTest01";
        String agentName2 = "agentTest02";

        Cluster cluster1 = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName1)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL)
                .build();

        Cluster cluster2 = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName2)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .setKill(KILL)
                .build();

        cluster1.start();
        cluster2.start();

        Sleep.start();
        try{
            List<String> processes = client.getChildren().forPath(cluster1.getProcessNodePath());
            List<String> agents = client.getChildren().forPath(cluster1.getAgentNodePath());

            System.out.println("cluster1, cluster2 start.");
            System.out.println(processes);
            System.out.println(agents);
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        cluster1.shutdown("test");

        Sleep.start();
        try{
            List<String> processes = client.getChildren().forPath(cluster1.getProcessNodePath());
            List<String> agents = client.getChildren().forPath(cluster1.getAgentNodePath());

            System.out.println("cluster1 shutdown.");
            System.out.println(processes);
            System.out.println(agents);
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }


        System.out.println("cluster1 start.");
        cluster1 = ClusterFactory.builder()
                .setClusterName(CLUSTER_NAME)
                .setAgentName(agentName1)
                .setZkHost(server.getConnectString())
                .setSessionTimeout(SESSION_TIMEOUT)
                .setMaxProcessors(maxProcessors)
                .setMaxRetries(RETRIES)
                .setCommand("top")
                .build();
        cluster1.start();
        Sleep.start();
        try{
            List<String> processes = client.getChildren().forPath(cluster1.getProcessNodePath());
            List<String> agents = client.getChildren().forPath(cluster1.getAgentNodePath());


            System.out.println(processes);
            System.out.println(agents);
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        cluster2.shutdown("test");
        Sleep.start();
        try{
            List<String> processes = client.getChildren().forPath(cluster1.getProcessNodePath());
            List<String> agents = client.getChildren().forPath(cluster1.getAgentNodePath());

            System.out.println("cluster2 shutdown.");
            System.out.println(processes);
            System.out.println(agents);
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        cluster1.shutdown("test");
        CloseableUtils.closeQuietly(server);
    }

    /**
     * maxProcessors: 1
     * start one agents,
     * check if there are one processor running.
     * */
    @Test
    public void test06_sessionExpired(){
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
        cluster.start();

        Sleep.start();

        List<String> processes = null;
        List<String> agents = null;
        String process = null;
        try{
            processes = client.getChildren().forPath(cluster.getProcessNodePath());
            agents = client.getChildren().forPath(cluster.getAgentNodePath());
            Assert.assertEquals(1, processes.size());
            process = processes.get(0);
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }


        System.out.println("kill session");
        try {
            KillSession.kill(cluster.getClient().getZookeeperClient().getZooKeeper(), server.getConnectString());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        Sleep.start();

        System.out.println("getChildren");
        try{
            processes = client.getChildren().forPath(cluster.getProcessNodePath());
            agents = client.getChildren().forPath(cluster.getAgentNodePath());
        }catch (Exception e){
            e.printStackTrace();
            Assert.assertNull(e);
        }

        Assert.assertEquals(maxProcessors, processes.size());
        Assert.assertEquals(1, agents.size());
        Assert.assertTrue(agents.get(0).endsWith(agentName));
        Assert.assertTrue(processes.get(0).endsWith(agentName));
        Assert.assertTrue(!process.equals(processes.get(0)));

        cluster.shutdown("Test");

        CloseableUtils.closeQuietly(server);
    }
}
