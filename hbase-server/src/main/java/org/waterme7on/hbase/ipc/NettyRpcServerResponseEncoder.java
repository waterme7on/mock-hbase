package org.waterme7on.hbase.ipc;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class NettyRpcServerResponseEncoder extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (msg instanceof RpcResponse) {
            RpcResponse resp = (RpcResponse) msg;
            BufferChain buf = resp.getResponse();
            ctx.write(Unpooled.wrappedBuffer(buf.getBuffers()), promise).addListener(f -> {
                resp.done();
            });
        } else {
            ctx.write(msg, promise);
        }
    }
}
