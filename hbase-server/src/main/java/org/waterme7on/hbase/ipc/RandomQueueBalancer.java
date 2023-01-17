package org.waterme7on.hbase.ipc;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;

/**
 * Queue balancer that just randomly selects a queue in the range [0, num
 * queues).
 */
public class RandomQueueBalancer implements QueueBalancer {
    private final int queueSize;
    private final List<BlockingQueue<CallRunner>> queues;

    public RandomQueueBalancer(Configuration conf, String executorName,
            List<BlockingQueue<CallRunner>> queues) {
        this.queueSize = queues.size();
        this.queues = queues;
    }

    @Override
    public int getNextQueue(CallRunner callRunner) {
        return ThreadLocalRandom.current().nextInt(queueSize);
    }

    /**
     * Exposed for use in tests
     */
    List<BlockingQueue<CallRunner>> getQueues() {
        return queues;
    }
}
