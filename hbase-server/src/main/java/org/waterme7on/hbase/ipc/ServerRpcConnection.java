package org.waterme7on.hbase.ipc;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.waterme7on.hbase.ipc.RpcServer.CallCleanup;

public abstract class ServerRpcConnection implements Closeable {
    protected final RpcServer rpcServer;
    // If the connection header has been read or not.
    protected boolean connectionHeaderRead = false;

    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    protected String hostAddress;
    protected int remotePort;
    protected InetAddress addr;
    protected ConnectionHeader connectionHeader;

    protected CallCleanup callCleanup;

    /**
     * Codec the client asked use.
     */
    protected Codec codec;
    /**
     * Compression codec the client asked us use.
     */
    protected CompressionCodec compressionCodec;

    public ServerRpcConnection(RpcServer rpcServer) {
        this.rpcServer = rpcServer;
        this.callCleanup = null;
    }

    @Override
    public String toString() {
        return getHostAddress() + ":" + remotePort;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public InetAddress getHostInetAddress() {
        return addr;
    }

    public int getRemotePort() {
        return remotePort;
    }

    protected final void callCleanupIfNeeded() {
        if (callCleanup != null) {
            callCleanup.run();
            callCleanup = null;
        }
    }

    public void processOneRpc(ByteBuff buf) throws IOException, InterruptedException {
        RpcServer.LOG.debug("processOneRpc");
        if (connectionHeaderRead) {
            processRequest(buf);
        } else {
            processConnectionHeader(buf);
            this.connectionHeaderRead = true;
        }
    }

    /**
     * Has the request header and the request param and optionally encoded data
     * buffer all in this one
     * array.
     * <p/>
     * Will be overridden in tests.
     */
    protected void processRequest(ByteBuff buf) throws IOException, InterruptedException {
    }

    // Reads the connection header following version
    private void processConnectionHeader(ByteBuff buf) throws IOException {
    }

}
