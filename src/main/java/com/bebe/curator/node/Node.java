package com.bebe.curator.node;

public interface Node {

    void create();

    void setData(byte[] data);

    boolean exists();

    void stop();

    void listen();
}
