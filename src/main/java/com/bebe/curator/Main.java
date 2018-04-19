package com.bebe.curator;

import com.bebe.curator.cluster.Cluster;

public class Main {

    public static void main(String[] args){
        Cluster cluster = new Cluster();
        cluster.start();

        while (true);
    }
}
