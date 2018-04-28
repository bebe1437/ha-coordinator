package com.bebe.curator.cluster;

import com.bebe.curator.process.Processor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateListener implements ConnectionStateListener {
    private Logger log = LoggerFactory.getLogger(getClass());
    private String name;
    private Cluster cluster;
    private Processor processor;

    public StateListener(String name, Cluster cluster){
        this.name = name;
        this.cluster = cluster;
    }

    public StateListener(String name, Cluster cluster, Processor processor){
        this.name = name;
        this.cluster = cluster;
        this.processor = processor;
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        log.info(String.format("%s:%s", name, connectionState));
        if(connectionState == ConnectionState.LOST){
            log.error("\t=== lost connection. ===");
            if(cluster!=null && processor==null) {
                int count = 0;
                while (true) {
                    try {
                        if (curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
                            log.info("\t=== cluster restart:{} ===", ++count);
                            cluster.restart();
                            break;
                        }
                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e) {

                    }
                }
            }

            if(processor!=null && processor.getProcessID()!=null){
                int count = 0;
                while (true) {
                    try {
                        if (curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
                            log.info("\t=== processor restart:{} ===", ++count);
                            processor.restart("lost-connection", cluster.getConf().getCommand());
                            break;
                        }
                    } catch (InterruptedException e) {
                        log.error("{}", e);
                        break;
                    } catch (Exception e) {
                        log.error("{}", e);
                        break;
                    }
                }
            }

        }
    }

}
