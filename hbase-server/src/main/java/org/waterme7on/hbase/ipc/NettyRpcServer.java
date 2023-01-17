package org.waterme7on.hbase.ipc;

import org.waterme7on.hbase.Server;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hbase.thirdparty.io.netty.channel.*;
import org.apache.hbase.thirdparty.io.netty.channel.group.ChannelGroup;
import org.apache.hbase.thirdparty.io.netty.channel.group.DefaultChannelGroup;
import org.apache.hbase.thirdparty.io.netty.handler.codec.FixedLengthFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.GlobalEventExecutor;

import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.regionserver.HRegionServer;
import org.waterme7on.hbase.util.NettyEventLoopGroupConfig;

public class NettyRpcServer extends RpcServer {
    public static final Logger LOG = LoggerFactory.getLogger(NettyRpcServer.class);
    public static final String HBASE_NETTY_ALLOCATOR_KEY = "hbase.netty.rpcserver.allocator";

    static final String POOLED_ALLOCATOR_TYPE = "pooled";
    static final String UNPOOLED_ALLOCATOR_TYPE = "unpooled";
    static final String HEAP_ALLOCATOR_TYPE = "heap";

    final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);

    private final ByteBufAllocator channelAllocator;
    private final Channel serverChannel;
    private final InetSocketAddress bindAddress;
    private final CountDownLatch closed = new CountDownLatch(1);

    /**
     * This flag is set to true after all threads are up and 'running' and the
     * server is then opened
     * for business by the call to {@link #start()}.
     */
    volatile boolean started = false;
    /**
     * This flag is used to indicate to sub threads when they should go down. When
     * we call
     * {@link #start()}, all threads started will consult this flag on whether they
     * should keep going.
     * It is set to false when {@link #stop()} is called.
     */
    volatile boolean running = true;

    public NettyRpcServer(Server server, String name, List<BlockingServiceAndInterface> services,
            InetSocketAddress bindAddress, Configuration conf, RpcScheduler scheduler,
            boolean reservoirEnabled) throws IOException {
        // initialize rpc server
        super(server, name, services, bindAddress, conf, scheduler, reservoirEnabled);
        LOG.debug("initializing NettyRpcServer");
        this.bindAddress = bindAddress;
        this.channelAllocator = getChannelAllocator(conf);
        NettyEventLoopGroupConfig config = null;
        if (server instanceof HRegionServer) {
            config = ((HRegionServer) server).getEventLoopGroupConfig();
        }
        if (config == null) {
            config = new NettyEventLoopGroupConfig(conf, "NettyRpcServer");
        }
        EventLoopGroup eventLoopGroup = config.group();
        Class<? extends ServerChannel> channelClass = config.serverChannelClass();
        ServerBootstrap bootstrap = new ServerBootstrap().group(eventLoopGroup).channel(channelClass)
                .childOption(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .childOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.config().setAllocator(channelAllocator);
                        ChannelPipeline pipeline = ch.pipeline();
                        FixedLengthFrameDecoder preambleDecoder = new FixedLengthFrameDecoder(6);
                        preambleDecoder.setSingleDecode(true);
                        pipeline.addLast("preambleDecoder", preambleDecoder);
                        pipeline.addLast("preambleHandler", createNettyRpcServerPreambleHandler());
                        pipeline.addLast("frameDecoder", new NettyRpcFrameDecoder(maxRequestSize));
                        pipeline.addLast("decoder", new NettyRpcServerRequestDecoder(allChannels));
                        pipeline.addLast("encoder", new NettyRpcServerResponseEncoder());
                    }
                });
        try {
            LOG.debug(name + " binding to " + bindAddress);
            serverChannel = bootstrap.bind(this.bindAddress).sync().channel();
            LOG.info("Bind to {}", serverChannel.localAddress());
        } catch (Exception e) {
            throw new IOException("Failed to bind to " + bindAddress, e);
        }
        this.scheduler.init(new RpcSchedulerContext(this));
    }

    private ByteBufAllocator getChannelAllocator(Configuration conf) throws IOException {
        final String value = conf.get(HBASE_NETTY_ALLOCATOR_KEY);
        if (value != null) {
            if (HEAP_ALLOCATOR_TYPE.equalsIgnoreCase(value)) {
                LOG.info("Using {} for buffer allocation", HeapByteBufAllocator.class.getName());
                return HeapByteBufAllocator.DEFAULT;
            } else {
                // If the value is none of the recognized labels, treat it as a class name. This
                // allows the
                // user to supply a custom implementation, perhaps for debugging.
                try {
                    // ReflectionUtils throws UnsupportedOperationException if there are any
                    // problems.
                    ByteBufAllocator alloc = (ByteBufAllocator) ReflectionUtils.newInstance(value);
                    LOG.info("Using {} for buffer allocation", value);
                    return alloc;
                } catch (ClassCastException | UnsupportedOperationException e) {
                    throw new IOException(e);
                }
            }
        }
        LOG.info("Using {} for buffer allocation", HeapByteBufAllocator.class.getName());
        return HeapByteBufAllocator.DEFAULT;
    }

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        LOG.debug("NettyRpcServer start to run");
        this.scheduler.start();
        started = true;
    };

    public boolean isStarted() {
        return this.started;
    };

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        LOG.info("Stopping server on " + this.serverChannel.localAddress());
        allChannels.close().awaitUninterruptibly();
        serverChannel.close();
        scheduler.stop();
        closed.countDown();
        running = false;
    }

    @Override
    public synchronized InetSocketAddress getListenerAddress() {
        return ((InetSocketAddress) serverChannel.localAddress());
    }

    @Override
    public String toString() {
        String name = this.getClass().getSimpleName() + ":" + getListenerAddress() + "(Connections Number:"
                + allChannels.size() + "), connections from {";
        StringBuilder sb = new StringBuilder(name);
        for (Channel channel : allChannels) {
            sb.append(channel.remoteAddress() + ",");
        }
        sb.append("}");
        return sb.toString();

    }

    // protected NettyRpcServerPreambleHandler createNettyRpcServerPreambleHandler()
    // {
    // return new NettyRpcServerPreambleHandler(NettyRpcServer.this);
    // }

    @Override
    public synchronized void join() throws InterruptedException {
        closed.await();
    }

    @Override
    public void setSocketSendBufSize(int size) {

    }

    protected NettyRpcServerPreambleHandler createNettyRpcServerPreambleHandler() {
        return new NettyRpcServerPreambleHandler(NettyRpcServer.this);
    }

    // @Override
    // public Pair<Message, CellScanner> call(RpcCall call) throws IOException {
    // return Super().call(call, null);
    // }

}
