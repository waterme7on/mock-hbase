package org.waterme7on.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.waterme7on.hbase.ipc.CallRunner;
import org.waterme7on.hbase.ipc.RpcScheduler;

public class SimpleRpcScheduler extends RpcScheduler {
    public SimpleRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount,
            int replicationHandlerCount, int highPriorityLevel) {
        this(conf, handlerCount, priorityHandlerCount, replicationHandlerCount, 0, null,
                highPriorityLevel);
    }

    public SimpleRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount,
            int replicationHandlerCount, int metaTransitionHandler,
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

    @Override
    public boolean dispatch(CallRunner task) throws InterruptedException {
        return false;
    }
}
