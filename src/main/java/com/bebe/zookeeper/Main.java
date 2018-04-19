package com.bebe.zookeeper;

import com.bebe.zookeeper.cluster.Cluster;

public class Main {
    private static final String PARAMETER_START_CLUSTER="start";

    public static void main(String[] args){

//        for(String arg: args){
//            if(arg.equals(PARAMETER_START_CLUSTER)){
//                new Cluster().start();
//                while (true);
//            }
//        }

        new Cluster().start();
        while (true);
    }
}
