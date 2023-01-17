package org.waterme7on.hbase.ipc;

import java.nio.ByteBuffer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;

/**
 * Handle connection preamble.
 * 
 * @since 2.0.0`
 */
class NettyRpcServerPreambleHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final NettyRpcServer rpcServer;

    public NettyRpcServerPreambleHandler(NettyRpcServer rpcServer) {
        NettyRpcServer.LOG.debug("construct NettyRpcServerPreambleHandler");
        this.rpcServer = rpcServer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        NettyRpcServer.LOG.debug("NettyRpcServerPreambleHandler.channelRead0");
        NettyServerRpcConnection conn = createNettyServerRpcConnection(ctx.channel());
        ByteBuffer buf = ByteBuffer.allocate(msg.readableBytes());
        msg.readBytes(buf);
        buf.flip();
        if (!conn.processPreamble(buf)) {
            conn.close();
            return;
        }
        ChannelPipeline p = ctx.pipeline();
        ((NettyRpcFrameDecoder) p.get("frameDecoder")).setConnection(conn);
        ((NettyRpcServerRequestDecoder) p.get("decoder")).setConnection(conn);
        p.remove(this);
        p.remove("preambleDecoder");
    }

    protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
        return new NettyServerRpcConnection(rpcServer, channel);
    }
}
