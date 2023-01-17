package org.waterme7on.hbase.ipc;

import java.net.InetSocketAddress;

public abstract class RpcScheduler {
    public static final String IPC_SERVER_MAX_CALLQUEUE_LENGTH = "hbase.ipc.server.max.callqueue.length";
    public static final String IPC_SERVER_PRIORITY_MAX_CALLQUEUE_LENGTH = "hbase.ipc.server.priority.max.callqueue.length";
    public static final String IPC_SERVER_REPLICATION_MAX_CALLQUEUE_LENGTH = "hbase.ipc.server.replication.max.callqueue.length";

    public static abstract class Context {
        public abstract InetSocketAddress getListenerAddress();
    }

    public abstract void init(Context context);

    /**
     * Prepares for request serving. An implementation may start some handler
     * threads here.
     */
    public abstract void start();

    /** Stops serving new requests. */
    public abstract void stop();

    public abstract boolean dispatch(CallRunner task) throws InterruptedException;
}