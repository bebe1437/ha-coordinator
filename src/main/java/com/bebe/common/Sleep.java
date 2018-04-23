package com.bebe.common;

public class Sleep {

    public static void start(){
        try {
            Thread.sleep(Configuration.getBufferTime());
        }catch (Exception e){}
    }
}
