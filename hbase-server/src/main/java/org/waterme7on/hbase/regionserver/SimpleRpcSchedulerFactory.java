package org.waterme7on.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Abortable;
import org.waterme7on.hbase.ipc.PriorityFunction;
import org.waterme7on.hbase.ipc.RpcScheduler;
import org.waterme7on.hbase.ipc.RpcSchedulerFactory;
public class SimpleRpcSchedulerFactory implements RpcSchedulerFactory {

    public RpcScheduler create(Configuration conf, PriorityFunction priority) {
        return create(conf, priority, null);
    }

    @Override
    public RpcScheduler create(Configuration conf, Abortable server) {
        return new SimpleRpcScheduler(conf, 1,
                conf.getInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT,
                        HConstants.DEFAULT_REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT),
                conf.getInt(HConstants.REGION_SERVER_REPLICATION_HANDLER_COUNT,
                        HConstants.DEFAULT_REGION_SERVER_REPLICATION_HANDLER_COUNT),
                conf.getInt(HConstants.MASTER_META_TRANSITION_HANDLER_COUNT,
                        HConstants.MASTER__META_TRANSITION_HANDLER_COUNT_DEFAULT),
                null, server, HConstants.QOS_THRESHOLD);
    }

    @Override
    public RpcScheduler create(Configuration conf, PriorityFunction priority, Abortable server) {
        int handlerCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
                HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
        return new SimpleRpcScheduler(conf, handlerCount,
                conf.getInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT,
                        HConstants.DEFAULT_REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT),
                conf.getInt(HConstants.REGION_SERVER_REPLICATION_HANDLER_COUNT,
                        HConstants.DEFAULT_REGION_SERVER_REPLICATION_HANDLER_COUNT),
                conf.getInt(HConstants.MASTER_META_TRANSITION_HANDLER_COUNT,
                        HConstants.MASTER__META_TRANSITION_HANDLER_COUNT_DEFAULT),
                priority, server, HConstants.QOS_THRESHOLD);
    }
}
