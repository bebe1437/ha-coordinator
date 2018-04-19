package com.bebe.common;

public class Sleep {

    public static void start(){
        try {
            Thread.sleep(Constants.BUFFER_TIME);
        }catch (Exception e){}
    }
}
