package org.waterme7on.hbase.regionserver;

import org.waterme7on.hbase.ipc.RpcExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.waterme7on.hbase.ipc.BalancedQueueRpcExecutor;
import org.waterme7on.hbase.ipc.CallRunner;
import org.waterme7on.hbase.ipc.PriorityFunction;
import org.waterme7on.hbase.ipc.RpcCall;
import org.waterme7on.hbase.ipc.RpcScheduler;
import org.waterme7on.hbase.ipc.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleRpcScheduler extends RpcScheduler implements ConfigurationObserver {

    public static final Logger LOG = LoggerFactory.getLogger(SimpleRpcScheduler.class);
    private int port;
    private final PriorityFunction priority;
    private final RpcExecutor callExecutor;

    public SimpleRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount, PriorityFunction priority,
            int replicationHandlerCount, int highPriorityLevel) {
        this(conf, handlerCount, priorityHandlerCount, replicationHandlerCount, 0, priority, null,
                highPriorityLevel);
    }

    public SimpleRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount,
            int replicationHandlerCount, int metaTransitionHandler, PriorityFunction priority,
            Abortable server, int highPriorityLevel) {

        this.priority = priority;
        int maxQueueLength = conf.getInt(RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH,
                handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);

        callExecutor = new BalancedQueueRpcExecutor("default.BQ", handlerCount, maxQueueLength,
                priority, conf, server);

    }

    /**
     * Resize call queues;
     * 
     * @param conf new configuration
     */
    @Override
    public void onConfigurationChange(Configuration conf) {
        // callExecutor.resizeQueues(conf);
        // String callQueueType = conf.get(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY,
        // RpcExecutor.CALL_QUEUE_TYPE_CONF_DEFAULT);
        // if (RpcExecutor.isCodelQueueType(callQueueType) ||
        // RpcExecutor.isPluggableQueueType(callQueueType)) {
        // callExecutor.onConfigurationChange(conf);
        // }
    }

    @Override
    public void init(Context context) {
        LOG.debug("init " + context.toString());
        this.port = context.getListenerAddress().getPort();
    }

    @Override
    public void start() {
        LOG.debug("fuckyouscheduler start at {}" + port);
        callExecutor.start(port);
    }

    @Override
    public void stop() {
        callExecutor.stop();
    }

    @Override
    public boolean dispatch(CallRunner callTask) throws InterruptedException {
        return callExecutor.dispatch(callTask);
    }
}
