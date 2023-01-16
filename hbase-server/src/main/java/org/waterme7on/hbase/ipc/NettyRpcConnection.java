package org.waterme7on.hbase.ipc;

import static org.waterme7on.hbase.ipc.CallEvent.Type.CANCELLED;
import static org.waterme7on.hbase.ipc.CallEvent.Type.TIMEOUT;
import static org.waterme7on.hbase.ipc.IPCUtil.execute;
import static org.waterme7on.hbase.ipc.IPCUtil.setCancelled;
import static org.waterme7on.hbase.ipc.IPCUtil.toIOE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.waterme7on.hbase.ipc.BufferCallBeforeInitHandler.BufferCallEvent;
import org.apache.hadoop.hbase.ipc.HBaseRpcController.CancellationCallback;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufOutputStream;
import org.apache.hbase.thirdparty.io.netty.buffer.Unpooled;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFutureListener;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOption;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleStateHandler;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCountUtil;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;

/**
 * RPC connection implementation based on netty.
 * <p/>
 * Most operations are executed in handlers. Netty handler is always executed in
 * the same
 * thread(EventLoop) so no lock is needed.
 * <p/>
 * <strong>Implementation assumptions:</strong> All the private methods should
 * be called in the
 * {@link #eventLoop} thread, otherwise there will be races.
 * 
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcConnection extends RpcConnection {

    private static final Logger LOG = LoggerFactory.getLogger(NettyRpcConnection.class);

    private final NettyRpcClient rpcClient;

    // the event loop used to set up the connection, we will also execute other
    // operations for this
    // connection in this event loop, to avoid locking everywhere.
    private final EventLoop eventLoop;

    private ByteBuf connectionHeaderPreamble;

    private ByteBuf connectionHeaderWithLength;

    // make it volatile so in the isActive method below we do not need to switch to
    // the event loop
    // thread to access this field.
    private volatile Channel channel;

    NettyRpcConnection(NettyRpcClient rpcClient, ConnectionId remoteId) throws IOException {
        super(rpcClient.conf, AbstractRpcClient.WHEEL_TIMER, remoteId, rpcClient.clusterId,
                rpcClient.userProvider.isHBaseSecurityEnabled(), rpcClient.codec, rpcClient.compressor,
                rpcClient.metrics);
        this.rpcClient = rpcClient;
        this.eventLoop = rpcClient.group.next();
        byte[] connectionHeaderPreamble = getConnectionHeaderPreamble();
        this.connectionHeaderPreamble = Unpooled.directBuffer(connectionHeaderPreamble.length)
                .writeBytes(connectionHeaderPreamble);
        ConnectionHeader header = getConnectionHeader();
        this.connectionHeaderWithLength = Unpooled.directBuffer(4 + header.getSerializedSize());
        this.connectionHeaderWithLength.writeInt(header.getSerializedSize());
        header.writeTo(new ByteBufOutputStream(this.connectionHeaderWithLength));
    }

    @Override
    protected void callTimeout(Call call) {
        execute(eventLoop, () -> {
            if (channel != null) {
                channel.pipeline().fireUserEventTriggered(new CallEvent(TIMEOUT, call));
            }
        });
    }

    @Override
    public boolean isActive() {
        return channel != null;
    }

    private void shutdown0() {
        assert eventLoop.inEventLoop();
        if (channel != null) {
            NettyFutureUtils.consume(channel.close());
            channel = null;
        }
    }

    @Override
    public void shutdown() {
        execute(eventLoop, this::shutdown0);
    }

    @Override
    public void cleanupConnection() {
        execute(eventLoop, () -> {
            if (connectionHeaderPreamble != null) {
                ReferenceCountUtil.safeRelease(connectionHeaderPreamble);
                connectionHeaderPreamble = null;
            }
            if (connectionHeaderWithLength != null) {
                ReferenceCountUtil.safeRelease(connectionHeaderWithLength);
                connectionHeaderWithLength = null;
            }
        });
    }

    private void established(Channel ch) throws IOException {
        assert eventLoop.inEventLoop();
        ch.pipeline()
                .addBefore(BufferCallBeforeInitHandler.NAME, null,
                        new IdleStateHandler(0, rpcClient.minIdleTimeBeforeClose, 0, TimeUnit.MILLISECONDS))
                .addBefore(BufferCallBeforeInitHandler.NAME, null,
                        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4))
                .addBefore(BufferCallBeforeInitHandler.NAME, null,
                        new NettyRpcDuplexHandler(this, rpcClient.cellBlockBuilder, codec, compressor))
                .fireUserEventTriggered(BufferCallEvent.success());
    }

    private boolean reloginInProgress;

    private void failInit(Channel ch, IOException e) {
        assert eventLoop.inEventLoop();
        // fail all pending calls
        ch.pipeline().fireUserEventTriggered(BufferCallEvent.fail(e));
        shutdown0();
    }

    private void connect() throws UnknownHostException {
        assert eventLoop.inEventLoop();
        LOG.trace("Connecting to {}", remoteId.getAddress());
        InetSocketAddress remoteAddr = getRemoteInetAddress(rpcClient.metrics);
        this.channel = new Bootstrap().group(eventLoop).channel(rpcClient.channelClass)
                .option(ChannelOption.TCP_NODELAY, rpcClient.isTcpNoDelay())
                .option(ChannelOption.SO_KEEPALIVE, rpcClient.tcpKeepAlive)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, rpcClient.connectTO)
                .handler(new ChannelInitializer<Channel>() {

                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(BufferCallBeforeInitHandler.NAME,
                                new BufferCallBeforeInitHandler());
                    }
                }).localAddress(rpcClient.localAddr).remoteAddress(remoteAddr).connect()
                .addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        Channel ch = future.channel();
                        if (!future.isSuccess()) {
                            IOException ex = toIOE(future.cause());
                            LOG.warn(
                                    "Exception encountered while connecting to the server " + remoteId.getAddress(),
                                    ex);
                            failInit(ch, ex);
                            // rpcClient.failedServers.addToFailedServers(remoteId.getAddress(),
                            // future.cause());
                            return;
                        }
                        NettyFutureUtils.safeWriteAndFlush(ch, connectionHeaderPreamble.retainedDuplicate());
                        // send the connection header to server
                        NettyFutureUtils.safeWrite(ch, connectionHeaderWithLength.retainedDuplicate());
                        established(ch);
                    }
                }).channel();
    }

    private void sendRequest0(Call call, HBaseRpcController hrc) throws IOException {
        assert eventLoop.inEventLoop();
        if (reloginInProgress) {
            throw new IOException("Can not send request because relogin is in progress.");
        }
        hrc.notifyOnCancel(new RpcCallback<Object>() {

            @Override
            public void run(Object parameter) {
                setCancelled(call);
                if (channel != null) {
                    channel.pipeline().fireUserEventTriggered(new CallEvent(CANCELLED, call));
                }
            }
        }, new CancellationCallback() {

            @Override
            public void run(boolean cancelled) throws IOException {
                if (cancelled) {
                    setCancelled(call);
                } else {
                    if (channel == null) {
                        connect();
                    }
                    scheduleTimeoutTask(call);
                    NettyFutureUtils.addListener(channel.writeAndFlush(call), new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            // Fail the call if we failed to write it out. This usually because the channel
                            // is
                            // closed. This is needed because we may shutdown the channel inside event loop
                            // and
                            // there may still be some pending calls in the event loop queue after us.
                            if (!future.isSuccess()) {
                                call.setException(toIOE(future.cause()));
                            }
                        }
                    });
                }
            }
        });
    }

    @Override
    public void sendRequest(final Call call, HBaseRpcController hrc) {
        execute(eventLoop, () -> {
            try {
                sendRequest0(call, hrc);
            } catch (Exception e) {
                call.setException(toIOE(e));
            }
        });
    }
}
