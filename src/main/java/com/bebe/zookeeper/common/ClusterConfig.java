package com.bebe.zookeeper.common;

import com.google.gson.annotations.SerializedName;

public class ClusterConfig {

    @SerializedName("p.cmd")
    private String command;
    @SerializedName("p.max")
    private int maxProcessors;

    public String getCommand() {
        return command;
    }

    public ClusterConfig setCommand(String command) {
        this.command = command;
        return this;
    }

    public int getMaxProcessors() {
        return maxProcessors;
    }

    public ClusterConfig setMaxProcessors(int maxProcessors) {
        this.maxProcessors = maxProcessors;
        return this;
    }
}
