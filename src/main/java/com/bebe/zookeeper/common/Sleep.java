package com.bebe.zookeeper.common;

public class Sleep {
    private static final long TIME = Configuration.getStopBufferTime();

    public static void start(){
        try {
            Thread.sleep(TIME);
        }catch (Exception e){}
    }
}
