package com.bebe.curator.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class Lock {
    protected Logger log = LoggerFactory.getLogger(getClass());
    private String lockPath;
    private long bufferTIme;

    public Lock(long bufferTime, String lockPath){
        this.lockPath = lockPath;
        this.bufferTIme = bufferTime;
    }

    public abstract CuratorFramework getClient();
    protected abstract void process();
    protected void before(){}
    protected void after(){}

    public void start(){
        before();

        log.info("\t=== acquire lock: {} ===", lockPath);

        CuratorFramework client = getClient();

        InterProcessMutex lock = new InterProcessMutex(client, lockPath);
        try {
            if(lock.acquire(bufferTIme, TimeUnit.MICROSECONDS)){
                process();
            }
        }catch (Exception e){
            log.error("\t=== fail to do lock:{} ===", e);
            client.close();
        }finally {
            try {
                lock.release();
                log.info("\t=== release lock: {} ===", lockPath);
            }catch (Exception e){
                log.debug("\t=== release lock:{} ===", e);
            }
            after();
        }
    }
}
