package com.bebe.curator.cache;

import java.util.Set;

public interface ChildrenCache {

    void process(Set<String> children);
}
