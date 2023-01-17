package org.waterme7on.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

public interface RpcSchedulerFactory {
    /**
     * Constructs a {@link RpcScheduler}.
     */
    RpcScheduler create(Configuration conf, PriorityFunction priority, Abortable server);

}
