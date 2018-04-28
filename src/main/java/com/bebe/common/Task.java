package com.bebe.common;

import com.google.gson.annotations.SerializedName;

import java.util.HashSet;
import java.util.Set;

public class Task {

    @SerializedName("cmd")
    private String command;
    @SerializedName("pl")
    private Set<String> runningProcessIDs;


    public Task(){
        runningProcessIDs = new HashSet<String>();
    }

    public Set<String> getRunningProcessIDs() {
        return runningProcessIDs;
    }

    public Task addRunningProcessID(Set<String> runningProcessIDs){
        this.runningProcessIDs.addAll(runningProcessIDs);
        return this;
    }

    public Task addRunningProcessID(String runningProcessID){
        this.runningProcessIDs.add(runningProcessID);
        return this;
    }

    public Task removeRunningProcessID(String processID){
        this.runningProcessIDs.remove(processID);
        return this;
    }

    public String getCommand() {
        return command;
    }

    public Task setCommand(String command) {
        this.command = command;
        return this;
    }
}
