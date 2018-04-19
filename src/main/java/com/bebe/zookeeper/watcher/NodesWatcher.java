package com.bebe.zookeeper.watcher;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class NodesWatcher implements Watcher{
    protected Logger log = LoggerFactory.getLogger(getClass());
    protected Set<String> childrenSet;
    private ZooKeeper zk;

    public NodesWatcher(ZooKeeper zk){
        this.zk = zk;
        childrenSet = new HashSet<String>();
    }

    AsyncCallback.ChildrenCallback callback = new AsyncCallback.ChildrenCallback() {

        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    check(path);
                    break;
                case OK:
                    run(children);
                    break;
                default:
                    log.error("\t=== Something went wrong[{}]:{} ===", rc, KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            check(watchedEvent.getPath());
        }
    }

    public void check(String path) {
        zk.getChildren(path
        , this
        , callback
        , null);
    }

    protected void run(final List<String> children){
        synchronized (childrenSet){
            for(String child: children){
                if(childrenSet.contains(child)){
                    continue;
                }
                childrenSet.add(child);
                log.info("\t=== {} created. ===", child);
            }
            if(children.size()!=childrenSet.size()){
                for(Iterator<String> it = childrenSet.iterator(); it.hasNext();){
                    String child = it.next();
                    if(!children.contains(child)){
                        log.error("\t=== {} deleted. ===", child);
                        it.remove();
                    }
                }
            }
        }
    }
}
