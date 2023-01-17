package org.waterme7on.hbase.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCountUtil;

public class NettyServerRpcConnection extends ServerRpcConnection {

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
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    void process(final ByteBuf buf) throws IOException, InterruptedException {
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
        process(new SingleByteBuff(buf));
    }

    void process(ByteBuff buf) throws IOException, InterruptedException {
        try {
            processOneRpc(buf);
        } catch (Exception e) {
            callCleanupIfNeeded();
            throw e;
        } finally {
            this.callCleanup = null;
        }
    }

}
