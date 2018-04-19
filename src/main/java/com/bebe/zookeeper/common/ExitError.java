package com.bebe.zookeeper.common;

public enum ExitError {
    SHUTDOWN(-1),
    ZK_CONNECTION(1),
    ZK_CREATE(2);

    private int code;

    ExitError(int code){
        this.code = code;
    }

    public int getCode(){
        return code;
    }

}
