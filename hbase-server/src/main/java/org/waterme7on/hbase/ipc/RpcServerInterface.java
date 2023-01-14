package org.waterme7on.hbase.ipc;

import org.waterme7on.hbase.regionserver.RSRpcServices;

import java.net.InetSocketAddress;

public interface RpcServerInterface {
    void start();

    boolean isStarted();

    void stop();

    void setRsRpcServices(RSRpcServices rsRpcServices);

    InetSocketAddress getListenerAddress();

    void setErrorHandler(HBaseRPCErrorHandler handler);

    void join() throws InterruptedException;

    String toString();

    void setSocketSendBufSize(int size);
}
