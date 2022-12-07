package org.waterme7on.hbase.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class StageManager {
    // stage event queue
    private BlockingQueue<String> eventQueue;
    // stage thread pools
    private ExecutorService threadPool;

    // methods for stage controller
    public abstract void run();
}
