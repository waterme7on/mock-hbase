package org.waterme7on.hbase.ipc;

import org.waterme7on.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.CellScanner;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

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

    void addCallSize(long size);

    Pair<Message, CellScanner> call(RpcCall call) throws IOException;

}
