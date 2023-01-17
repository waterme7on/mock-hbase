package org.waterme7on.hbase.ipc;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.waterme7on.hbase.ipc.RpcServer.CallCleanup;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * Datastructure that holds all necessary to a method invocation and then
 * afterward, carries the
 * result.
 * 
 * @since 2.0.0
 */
class NettyServerCall extends ServerCall<NettyServerRpcConnection> {

    NettyServerCall(int id, BlockingService service, MethodDescriptor md, RequestHeader header,
            Message param, CellScanner cellScanner, NettyServerRpcConnection connection, long size,
            InetAddress remoteAddress, long receiveTime, int timeout, ByteBuffAllocator bbAllocator,
            CellBlockBuilder cellBlockBuilder, CallCleanup reqCleanup) {
        super(id, service, md, header, param, cellScanner, connection, size, remoteAddress, receiveTime,
                timeout, bbAllocator, cellBlockBuilder, reqCleanup);
    }

    /**
     * If we have a response, and delay is not set, then respond immediately.
     * Otherwise, do not
     * respond to client. This is called by the RPC code in the context of the
     * Handler thread.
     */
    @Override
    public synchronized void sendResponseIfReady() throws IOException {
        // set param null to reduce memory pressure
        this.param = null;
        connection.channel.writeAndFlush(this);
    }
}
