package org.waterme7on.hbase.wal;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutput;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.wal.NettyAsyncFSWALConfigHelper;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;

/**
 * A WAL provider that use {@link AsyncFSWAL}.
 */
public class AsyncFSWALProvider extends AbstractFSWALProvider<AsyncFSWAL> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncFSWALProvider.class);

    public static final String WRITER_IMPL = "hbase.regionserver.hlog.async.writer.impl";

    private EventLoopGroup eventLoopGroup;

    private Class<? extends Channel> channelClass;

    @Override
    protected AsyncFSWAL createWAL() throws IOException {
        return new AsyncFSWAL(FileSystem.get(conf), this.abortable,
                CommonFSUtils.getWALRootDir(conf), getWALDirectoryName(factory.factoryId),
                getWALArchiveDirectoryName(conf, factory.factoryId), conf, listeners, true, logPrefix,
                META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null, eventLoopGroup,
                channelClass, factory.getExcludeDatanodeManager().getStreamSlowMonitor(providerId));
    }

    @Override
    protected void doInit(Configuration conf) throws IOException {
        eventLoopGroup = new NioEventLoopGroup(1,
                new DefaultThreadFactory("AsyncFSWAL", true, Thread.MAX_PRIORITY));
        channelClass = NioSocketChannel.class;
    }

    /**
     * Public because of AsyncFSWAL. Should be package-private
     */
    public static AsyncWriter createAsyncWriter(Configuration conf, FileSystem fs, Path path,
            boolean overwritable, EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass)
            throws IOException {
        return createAsyncWriter(conf, fs, path, overwritable, WALUtil.getWALBlockSize(conf, fs, path),
                eventLoopGroup, channelClass, StreamSlowMonitor.create(conf, path.getName()));
    }

    // Only public so classes back in regionserver.wal can access
    public interface AsyncWriter extends WALProvider.AsyncWriter {
        /**
         * @throws IOException                    if something goes wrong initializing
         *                                        an output stream
         * @throws StreamLacksCapabilityException if the given FileSystem can't provide
         *                                        streams that
         *                                        meet the needs of the given Writer
         *                                        implementation.
         */
        void init(FileSystem fs, Path path, Configuration c, boolean overwritable, long blocksize,
                StreamSlowMonitor monitor) throws IOException, CommonFSUtils.StreamLacksCapabilityException;
    }

    /**
     * Public because of AsyncFSWAL. Should be package-private
     */
    public static AsyncWriter createAsyncWriter(Configuration conf, FileSystem fs, Path path,
            boolean overwritable, long blocksize, EventLoopGroup eventLoopGroup,
            Class<? extends Channel> channelClass, StreamSlowMonitor monitor) throws IOException {
        // Configuration already does caching for the Class lookup.

        try {
            AsyncWriter writer = AsyncProtobufLogWriter.class
                    .getConstructor(EventLoopGroup.class, Class.class)
                    .newInstance(eventLoopGroup, channelClass);
            writer.init(fs, path, conf, overwritable, blocksize, monitor);
            return writer;
        } catch (Exception e) {
            if (e instanceof CommonFSUtils.StreamLacksCapabilityException) {
                LOG.error("The RegionServer async write ahead log provider "
                        + "relies on the ability to call " + e.getMessage() + " for proper operation during "
                        + "component failures, but the current FileSystem does not support doing so. Please "
                        + "check the config value of '" + CommonFSUtils.HBASE_WAL_DIR + "' and ensure "
                        + "it points to a FileSystem mount that has suitable capabilities for output streams.");
            } else {
                LOG.debug("Error instantiating log writer.", e);
            }
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException("cannot get log writer", e);
        }
    }

    /**
     * Test whether we can load the helper classes for async dfs output.
     */
    public static boolean load() {
        try {
            Class.forName(FanOutOneBlockAsyncDFSOutput.class.getName());
            Class.forName(FanOutOneBlockAsyncDFSOutputHelper.class.getName());
            Class.forName(FanOutOneBlockAsyncDFSOutputSaslHelper.class.getName());
            return true;
        } catch (Throwable e) {
            return false;
        }
    }
}