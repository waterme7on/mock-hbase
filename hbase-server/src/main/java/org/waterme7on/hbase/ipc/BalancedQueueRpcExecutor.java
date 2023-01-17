package org.waterme7on.hbase.ipc;

import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.waterme7on.hbase.regionserver.SimpleRpcScheduler;

/**
 * An {@link RpcExecutor} that will balance requests evenly across all its
 * queues, but still remains
 * efficient with a single queue via an inlinable queue balancing mechanism.
 * Defaults to FIFO but
 * you can pass an alternate queue class to use.
 */
public class BalancedQueueRpcExecutor extends RpcExecutor {

    private final QueueBalancer balancer;

    public BalancedQueueRpcExecutor(final String name, final int handlerCount,
            final int maxQueueLength, final PriorityFunction priority, final Configuration conf,
            final Abortable abortable) {
        this(name, handlerCount, conf.get(CALL_QUEUE_TYPE_CONF_KEY, CALL_QUEUE_TYPE_CONF_DEFAULT),
                maxQueueLength, priority, conf, abortable);
    }

    public BalancedQueueRpcExecutor(final String name, final int handlerCount,
            final String callQueueType, final int maxQueueLength, final PriorityFunction priority,
            final Configuration conf, final Abortable abortable) {
        super(name, handlerCount, callQueueType, maxQueueLength, priority, conf, abortable);
        initializeQueues(this.numCallQueues);
        this.balancer = getBalancer(name, conf, getQueues());
    }

    @Override
    public boolean dispatch(final CallRunner callTask) {
        SimpleRpcScheduler.LOG.debug("BQExecutor - dispatch " + callTask.toString());
        int queueIndex = balancer.getNextQueue(callTask);
        BlockingQueue<CallRunner> queue = queues.get(queueIndex);
        // that means we can overflow by at most <num reader> size (5), that's ok
        if (queue.size() >= currentQueueLimit) {
            return false;
        }
        return queue.offer(callTask);
    }

    @Override
    public void onConfigurationChange(Configuration conf) {
        super.onConfigurationChange(conf);

        if (balancer instanceof ConfigurationObserver) {
            ((ConfigurationObserver) balancer).onConfigurationChange(conf);
        }
    }
}
