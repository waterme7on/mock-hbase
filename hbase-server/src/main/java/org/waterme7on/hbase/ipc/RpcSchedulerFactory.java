package org.waterme7on.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.waterme7on.hbase.Abortable;
public interface RpcSchedulerFactory {
    /**
     * Constructs a {@link org.apache.hadoop.hbase.ipc.RpcScheduler}.
     */
    RpcScheduler create(Configuration conf, Abortable server);
}
