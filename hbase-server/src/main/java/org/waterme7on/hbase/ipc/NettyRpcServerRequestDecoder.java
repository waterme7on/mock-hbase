package org.waterme7on.hbase.ipc;

import org.apache.yetus.audience.InterfaceAudience;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;

class NettyRpcServerRequestDecoder extends ChannelInboundHandlerAdapter {
    private final ChannelGroup allChannels;

    public NettyRpcServerRequestDecoder(ChannelGroup allChannels) {
        this.allChannels = allChannels;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        allChannels.add(ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf input = (ByteBuf) msg;
        // TODO
        // connection.process(input);
        while (input.isReadable()) {
            System.out.println(input.readByte());
        }
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
