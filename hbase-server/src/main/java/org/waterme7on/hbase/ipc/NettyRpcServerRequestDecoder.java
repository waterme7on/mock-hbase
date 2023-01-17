package org.waterme7on.hbase.ipc;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.channel.group.ChannelGroup;

class NettyRpcServerRequestDecoder extends ChannelInboundHandlerAdapter {
    private final ChannelGroup allChannels;

    public NettyRpcServerRequestDecoder(ChannelGroup allChannels) {
        this.allChannels = allChannels;
    }

    private NettyServerRpcConnection connection;

    void setConnection(NettyServerRpcConnection connection) {
        this.connection = connection;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        allChannels.add(ctx.channel());
        NettyRpcServer.LOG.trace("Connection {}; # active connections={}",
                ctx.channel().remoteAddress(), (allChannels.size() - 1));
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf input = (ByteBuf) msg;
        connection.process(input);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        allChannels.remove(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        allChannels.remove(ctx.channel());
        ctx.channel().close();
    }
}
