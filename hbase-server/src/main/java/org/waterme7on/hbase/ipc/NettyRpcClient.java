package org.waterme7on.hbase.ipc;

import java.io.IOException;
import java.net.SocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Netty client for the requests and responses.
 * 
 * @since 2.0.0
 */
public class NettyRpcClient extends AbstractRpcClient<NettyRpcConnection> {

    final EventLoopGroup group;

    final Class<? extends Channel> channelClass;

    private final boolean shutdownGroupWhenClose;

    public NettyRpcClient(Configuration configuration, String clusterId, SocketAddress localAddress,
            MetricsConnection metrics) {
        super(configuration, clusterId, localAddress, metrics);
        Pair<EventLoopGroup, Class<? extends Channel>> groupAndChannelClass = NettyRpcClientConfigHelper
                .getEventLoopConfig(conf);
        if (groupAndChannelClass == null) {
            // Use our own EventLoopGroup.
            int threadCount = conf.getInt(NettyRpcClientConfigHelper.HBASE_NETTY_EVENTLOOP_RPCCLIENT_THREADCOUNT_KEY,
                    0);
            this.group = new NioEventLoopGroup(threadCount,
                    new DefaultThreadFactory("RPCClient(own)-NioEventLoopGroup", true, Thread.NORM_PRIORITY));
            this.channelClass = NioSocketChannel.class;
            this.shutdownGroupWhenClose = true;
        } else {
            this.group = groupAndChannelClass.getFirst();
            this.channelClass = groupAndChannelClass.getSecond();
            this.shutdownGroupWhenClose = false;
        }
    }

    /** Used in test only. */
    NettyRpcClient(Configuration configuration) {
        this(configuration, HConstants.CLUSTER_ID_DEFAULT, null, null);
    }

    @Override
    protected NettyRpcConnection createConnection(ConnectionId remoteId) throws IOException {
        return new NettyRpcConnection(this, remoteId);
    }

    @Override
    protected void closeInternal() {
        if (shutdownGroupWhenClose) {
            NettyFutureUtils.consume(group.shutdownGracefully());
        }
    }

}
