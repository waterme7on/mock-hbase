package org.waterme7on.hbase.ipc;

public interface RpcServerInterface {
    void start();

    boolean isStarted();

    void stop();
}
