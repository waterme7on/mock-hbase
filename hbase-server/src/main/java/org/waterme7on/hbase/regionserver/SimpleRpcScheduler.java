package org.waterme7on.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.waterme7on.hbase.ipc.RpcScheduler;
import org.waterme7on.hbase.ipc.PriorityFunction;

public class SimpleRpcScheduler extends RpcScheduler {
    public SimpleRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount,
                              int replicationHandlerCount, PriorityFunction priority, int highPriorityLevel) {
        this(conf, handlerCount, priorityHandlerCount, replicationHandlerCount, 0, priority, null,
                highPriorityLevel);
    }

    public SimpleRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount,
                              int replicationHandlerCount, int metaTransitionHandler, PriorityFunction priority,
                              Abortable server, int highPriorityLevel) {

    }
    @Override
    public void init(Context context) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
