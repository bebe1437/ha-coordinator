package com.bebe.common;

import com.google.gson.annotations.SerializedName;

public class ClusterConfig {

    @SerializedName("p.cmd")
    private String command;
    @SerializedName("p.max")
    private int maxProcessors;
    @SerializedName("p.kill")
    private String kill;

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

    public String getKill() {
        return kill;
    }

    public ClusterConfig setKill(String kill) {
        this.kill = kill;
        return this;
    }
}
