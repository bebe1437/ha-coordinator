package com.bebe.curator.cache;

public interface NodeCache extends Cache {

    void process(byte[] data);

    void remove();
}
