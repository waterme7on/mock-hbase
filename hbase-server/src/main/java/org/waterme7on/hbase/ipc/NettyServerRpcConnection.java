package org.waterme7on.hbase.ipc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ipc.FatalConnectionException;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.ipc.RpcServer.CallCleanup;

import static org.apache.hadoop.hbase.HConstants.RPC_HEADER;

public class NettyServerRpcConnection extends ServerRpcConnection {

    private static final Logger LOG = LoggerFactory.getLogger(NettyServerRpcConnection.class);
    final Channel channel;

    public NettyServerRpcConnection(RpcServer rpcServer, Channel channel) {
        super(rpcServer);
        this.channel = channel;
        channel.closeFuture().addListener(f -> {
            callCleanupIfNeeded();
        });
        InetSocketAddress inetSocketAddress = ((InetSocketAddress) channel.remoteAddress());
        this.addr = inetSocketAddress.getAddress();
        if (addr == null) {
            this.hostAddress = "*Unknown*";
        } else {
            this.hostAddress = inetSocketAddress.getAddress().getHostAddress();
        }
        this.remotePort = inetSocketAddress.getPort();
        LOG.debug("received new connection from " + this);
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    void process(final ByteBuf buf) throws IOException, InterruptedException {
        LOG.debug("1");
        if (connectionHeaderRead) {
            this.callCleanup = () -> ReferenceCountUtil.safeRelease(buf);
            process(new SingleByteBuff(buf.nioBuffer()));
        } else {
            ByteBuffer connectionHeader = ByteBuffer.allocate(buf.readableBytes());
            try {
                buf.readBytes(connectionHeader);
            } finally {
                buf.release();
            }
            process(connectionHeader);
        }
    }

    void process(ByteBuffer buf) throws IOException, InterruptedException {
        LOG.debug("2");
        process(new SingleByteBuff(buf));
    }

    protected final boolean processPreamble(ByteBuffer preambleBuffer) throws IOException {
        assert preambleBuffer.remaining() == 6;
        for (int i = 0; i < RPC_HEADER.length; i++) {
            if (RPC_HEADER[i] != preambleBuffer.get()) {
                doBadPreambleHandling(
                        "Expected HEADER=" + Bytes.toStringBinary(RPC_HEADER) + " but received HEADER="
                                + Bytes.toStringBinary(preambleBuffer.array(), 0, RPC_HEADER.length) + " from "
                                + toString());
                return false;
            }
        }
        return true;
    }

    private void doBadPreambleHandling(String msg) throws IOException {
        doBadPreambleHandling(msg, new FatalConnectionException(msg));
    }

    private void doBadPreambleHandling(String msg, Exception e) throws IOException {
        LOG.warn(msg);
    }

    void process(ByteBuff buf) throws IOException, InterruptedException {
        LOG.debug("3");
        try {
            processOneRpc(buf);
        } catch (Exception e) {
            callCleanupIfNeeded();
            throw e;
        } finally {
            this.callCleanup = null;
        }
    }

    @Override
    public NettyServerCall createCall(int id, final BlockingService service,
            final MethodDescriptor md, RequestHeader header, Message param, CellScanner cellScanner,
            long size, final InetAddress remoteAddress, int timeout, CallCleanup reqCleanup) {
        return new NettyServerCall(id, service, md, header, param, cellScanner, this, size,
                remoteAddress, EnvironmentEdgeManager.currentTime(), timeout, this.rpcServer.bbAllocator,
                this.rpcServer.cellBlockBuilder, reqCleanup);
    }

    @Override
    protected void doRespond(RpcResponse resp) {
        channel.writeAndFlush(resp);
    }

    @Override
    public boolean isConnectionOpen() {
        return channel.isOpen();
    }

}
