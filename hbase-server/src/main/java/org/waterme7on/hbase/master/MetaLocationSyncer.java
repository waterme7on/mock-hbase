package org.waterme7on.hbase.master;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.waterme7on.hbase.Server;
import org.apache.hadoop.hbase.master.zksyncer.ClientZKSyncer;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
/**
 * Tracks the meta region locations on server ZK cluster and synchronize them to client ZK cluster
 * if changed
 */
public class MetaLocationSyncer extends ClientZKSyncer {

    private volatile int metaReplicaCount = 1;

    public MetaLocationSyncer(ZKWatcher watcher, ZKWatcher clientZkWatcher, Server server) {
        super(watcher, clientZkWatcher, null);
    }

    @Override
    protected boolean validate(String path) {
        return watcher.getZNodePaths().isMetaZNodePath(path);
    }

    @Override
    protected Set<String> getPathsToWatch() {
        return IntStream.range(0, metaReplicaCount)
                .mapToObj(watcher.getZNodePaths()::getZNodeForReplica).collect(Collectors.toSet());
    }

    public void setMetaReplicaCount(int replicaCount) {
        if (replicaCount != metaReplicaCount) {
            metaReplicaCount = replicaCount;
            refreshWatchingList();
        }
    }
}
