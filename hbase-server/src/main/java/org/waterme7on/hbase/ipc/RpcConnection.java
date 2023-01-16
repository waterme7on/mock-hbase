package org.waterme7on.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;
import org.apache.hbase.thirdparty.io.netty.util.TimerTask;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
// import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

abstract class RpcConnection {
    private static final Logger LOG = LoggerFactory.getLogger(RpcConnection.class);
    protected final ConnectionId remoteId;
    protected final Configuration conf;
    protected final HashedWheelTimer timeoutTimer;
    protected static String CRYPTO_AES_ENABLED_KEY = "hbase.rpc.crypto.encryption.aes.enabled";
    protected static boolean CRYPTO_AES_ENABLED_DEFAULT = false;
    protected final Codec codec;
    protected final CompressionCodec compressor;
    // the last time we were picked up from connection pool.
    protected long lastTouched;

    protected RpcConnection(Configuration conf, HashedWheelTimer wheelTimer, ConnectionId remoteId,
            String clusterId, boolean isSecurityEnabled, Codec codec, CompressionCodec compressor,
            MetricsConnection metrics) throws IOException {
        this.codec = codec;
        this.compressor = compressor;
        this.remoteId = remoteId;
        this.conf = conf;
        this.timeoutTimer = wheelTimer;
        LOG.debug("service={}, clusterId={}, isSecurityEnabled={}, codec={}, compressor={}",
                remoteId.getServiceName(), clusterId, isSecurityEnabled, codec, compressor);
    }

    protected final void scheduleTimeoutTask(final Call call) {
        if (call.timeout > 0) {
            call.timeoutTask = timeoutTimer.newTimeout(new TimerTask() {

                @Override
                public void run(Timeout timeout) throws Exception {
                    call.setTimeout(new CallTimeoutException(call.toShortString() + ", waitTime="
                            + (EnvironmentEdgeManager.currentTime() - call.getStartTime()) + "ms, rpcTimeout="
                            + call.timeout + "ms"));
                    callTimeout(call);
                }
            }, call.timeout, TimeUnit.MILLISECONDS);
        }
    }

    protected final byte[] getConnectionHeaderPreamble() {
        // Assemble the preamble up in a buffer first and then send it. Writing
        // individual elements,
        // they are getting sent across piecemeal according to wireshark and then server
        // is messing
        // up the reading on occasion (the passed in stream is not buffered yet).

        // Preamble is six bytes -- 'HBas' + VERSION + AUTH_CODE
        int rpcHeaderLen = HConstants.RPC_HEADER.length;
        byte[] preamble = new byte[rpcHeaderLen + 2];
        System.arraycopy(HConstants.RPC_HEADER, 0, preamble, 0, rpcHeaderLen);
        preamble[rpcHeaderLen] = HConstants.RPC_CURRENT_VERSION;
        // synchronized (this) {
        // preamble[rpcHeaderLen + 1] = provider.getSaslAuthMethod().getCode();
        // }
        return preamble;
    }

    protected final ConnectionHeader getConnectionHeader() {
        final ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
        builder.setServiceName(remoteId.getServiceName());
        // final UserInformation userInfoPB = provider.getUserInfo(remoteId.ticket);
        // if (userInfoPB != null) {
        // builder.setUserInfo(userInfoPB);
        // }
        if (this.codec != null) {
            builder.setCellBlockCodecClass(this.codec.getClass().getCanonicalName());
        }
        if (this.compressor != null) {
            builder.setCellBlockCompressorClass(this.compressor.getClass().getCanonicalName());
        }
        // builder.setVersionInfo(ProtobufUtil.getVersionInfo());
        boolean isCryptoAESEnable = conf.getBoolean(CRYPTO_AES_ENABLED_KEY, CRYPTO_AES_ENABLED_DEFAULT);
        // if Crypto AES enable, setup Cipher transformation
        if (isCryptoAESEnable) {
            builder.setRpcCryptoCipherTransformation(
                    conf.get("hbase.rpc.crypto.encryption.aes.cipher.transform", "AES/CTR/NoPadding"));
        }
        return builder.build();
    }

    protected final InetSocketAddress getRemoteInetAddress(MetricsConnection metrics)
            throws UnknownHostException {
        if (metrics != null) {
            metrics.incrNsLookups();
        }
        InetSocketAddress remoteAddr = Address.toSocketAddress(remoteId.getAddress());
        if (remoteAddr.isUnresolved()) {
            if (metrics != null) {
                metrics.incrNsLookupsFailed();
            }
            throw new UnknownHostException(remoteId.getAddress() + " could not be resolved");
        }
        return remoteAddr;
    }

    protected abstract void callTimeout(Call call);

    public ConnectionId remoteId() {
        return remoteId;
    }

    public long getLastTouched() {
        return lastTouched;
    }

    public void setLastTouched(long lastTouched) {
        this.lastTouched = lastTouched;
    }

    /**
     * Tell the idle connection sweeper whether we could be swept.
     */
    public abstract boolean isActive();

    /**
     * Just close connection. Do not need to remove from connection pool.
     */
    public abstract void shutdown();

    public abstract void sendRequest(Call call, HBaseRpcController hrc) throws IOException;

    /**
     * Does the clean up work after the connection is removed from the connection
     * pool
     */
    public abstract void cleanupConnection();
}