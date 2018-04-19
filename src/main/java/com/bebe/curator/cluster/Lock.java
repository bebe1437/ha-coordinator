package com.bebe.curator.cluster;

import com.bebe.common.Constants;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class Lock {
    protected Logger log = LoggerFactory.getLogger(getClass());
    private String lockPath;

    public Lock(String lockPath){
        this.lockPath = lockPath;
    }

    protected abstract void before();
    protected abstract CuratorFramework getClient();
    protected void after(){}
    protected abstract void process();

    public void start(){
        log.info("\t=== start locking... ===");
        before();

        CuratorFramework client = getClient();

        InterProcessMutex lock = new InterProcessMutex(client, lockPath);
        try {
            if(lock.acquire(Constants.BUFFER_TIME, TimeUnit.MICROSECONDS)){
                process();
            }
        }catch (Exception e){
            log.error("\t=== fail to do lock:{} ===", e);
            client.close();
        }finally {
            try {
                lock.release();
                log.info("\t=== release lock ===");
            }catch (Exception e){
                log.debug("\t=== release lock:{} ===", e);
            }
            after();
        }
    }
}
