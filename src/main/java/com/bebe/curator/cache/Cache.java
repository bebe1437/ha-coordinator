package com.bebe.curator.cache;

import java.io.Closeable;

public interface Cache extends Closeable{

    void listen() throws Exception;
}
