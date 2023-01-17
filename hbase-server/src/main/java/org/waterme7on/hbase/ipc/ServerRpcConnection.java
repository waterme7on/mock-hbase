package org.waterme7on.hbase.ipc;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownServiceException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.ipc.FatalConnectionException;
import org.apache.hadoop.hbase.ipc.UnsupportedCellCodecException;
import org.apache.hadoop.hbase.ipc.UnsupportedCompressionCodecException;
import org.apache.hadoop.hbase.ipc.UnsupportedCryptoException;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TracingProtos.RPCTInfo;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteInput;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.waterme7on.hbase.ipc.RpcServer.CallCleanup;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ServerRpcConnection implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerRpcConnection.class);
    protected final RpcServer rpcServer;
    // If the connection header has been read or not.
    protected boolean connectionHeaderRead = false;
    private static final TextMapGetter<RPCTInfo> getter = new RPCTInfoGetter();

    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    protected String hostAddress;
    protected int remotePort;
    protected InetAddress addr;
    protected ConnectionHeader connectionHeader;
    protected BlockingService service;
    protected CallCleanup callCleanup;
    protected CryptoAES cryptoAES;
    protected boolean useWrap = false;
    protected boolean useCryptoAesWrap = false;
    protected User user = null;
    protected boolean retryImmediatelySupported = false;

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
        RpcServer.LOG.debug("ServerRpcConnection.processOneRpc");
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
        LOG.debug("ServerRpcConnection.processRequest");
        long totalRequestSize = buf.limit();
        int offset = 0;
        // Here we read in the header. We avoid having pb
        // do its default 4k allocation for CodedInputStream. We force it to use
        // backing array.
        CodedInputStream cis;
        if (buf.hasArray()) {
            cis = UnsafeByteOperations.unsafeWrap(buf.array(), 0, buf.limit()).newCodedInput();
        } else {
            cis = UnsafeByteOperations.unsafeWrap(new ByteBuffByteInput(buf, 0, buf.limit()), 0, buf.limit())
                    .newCodedInput();
        }
        cis.enableAliasing(true);
        int headerSize = cis.readRawVarint32();
        offset = cis.getTotalBytesRead();
        Message.Builder builder = RequestHeader.newBuilder();
        ProtobufUtil.mergeFrom(builder, cis, headerSize);
        RequestHeader header = (RequestHeader) builder.build();
        offset += headerSize;
        Context traceCtx = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), header.getTraceInfo(), getter);
        // n.b. Management of this Span instance is a little odd. Most exit paths from
        // this try scope
        // are early-exits due to error cases. There's only one success path, the
        // asynchronous call to
        // RpcScheduler#dispatch. The success path assumes ownership of the span, which
        // is represented
        // by null-ing out the reference in this scope. All other paths end the span.
        // Thus, and in
        // order to avoid accidentally orphaning the span, the call to Span#end happens
        // in a finally
        // block iff the span is non-null.
        Span span = TraceUtil.createRemoteSpan("RpcServer.process", traceCtx);
        try (Scope ignored = span.makeCurrent()) {
            int id = header.getCallId();
            if (RpcServer.LOG.isTraceEnabled()) {
                RpcServer.LOG.trace("RequestHeader " + TextFormat.shortDebugString(header)
                        + " totalRequestSize: " + totalRequestSize + " bytes");
            }
            // Enforcing the call queue size, this triggers a retry in the client
            // This is a bit late to be doing this check - we have already read in the
            // total request.
            if ((totalRequestSize + this.rpcServer.callQueueSizeInBytes.sum()) > this.rpcServer.maxQueueSizeInBytes) {
                final ServerCall<?> callTooBig = createCall(id, this.service, null, null, null, null,
                        totalRequestSize, null, 0, this.callCleanup);
                // this.rpcServer.metrics.exception(RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
                callTooBig.setResponse(null, null, RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION,
                        "Call queue is full on " + this.rpcServer.server.getServerName()
                                + ", is hbase.ipc.server.max.callqueue.size too small?");
                TraceUtil.setError(span, RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
                callTooBig.sendResponseIfReady();
                return;
            }
            MethodDescriptor md = null;
            Message param = null;
            CellScanner cellScanner = null;
            try {
                if (header.hasRequestParam() && header.getRequestParam()) {
                    md = this.service.getDescriptorForType().findMethodByName(header.getMethodName());
                    if (md == null) {
                        throw new UnsupportedOperationException(header.getMethodName());
                    }
                    builder = this.service.getRequestPrototype(md).newBuilderForType();
                    cis.resetSizeCounter();
                    int paramSize = cis.readRawVarint32();
                    offset += cis.getTotalBytesRead();
                    if (builder != null) {
                        ProtobufUtil.mergeFrom(builder, cis, paramSize);
                        param = builder.build();
                    }
                    offset += paramSize;
                } else {
                    // currently header must have request param, so we directly throw
                    // exception here
                    String msg = "Invalid request header: " + TextFormat.shortDebugString(header)
                            + ", should have param set in it";
                    RpcServer.LOG.warn(msg);
                    throw new DoNotRetryIOException(msg);
                }
                if (header.hasCellBlockMeta()) {
                    buf.position(offset);
                    ByteBuff dup = buf.duplicate();
                    dup.limit(offset + header.getCellBlockMeta().getLength());
                    cellScanner = this.rpcServer.cellBlockBuilder.createCellScannerReusingBuffers(this.codec,
                            this.compressionCodec, dup);
                }
            } catch (Throwable thrown) {
                InetSocketAddress address = this.rpcServer.getListenerAddress();
                String msg = (address != null ? address : "(channel closed)")
                        + " is unable to read call parameter from client " + getHostAddress();
                RpcServer.LOG.warn(msg, thrown);

                // this.rpcServer.metrics.exception(thrown);

                final Throwable responseThrowable;
                if (thrown instanceof LinkageError) {
                    // probably the hbase hadoop version does not match the running hadoop version
                    responseThrowable = new DoNotRetryIOException(thrown);
                } else if (thrown instanceof UnsupportedOperationException) {
                    // If the method is not present on the server, do not retry.
                    responseThrowable = new DoNotRetryIOException(thrown);
                } else {
                    responseThrowable = thrown;
                }

                ServerCall<?> readParamsFailedCall = createCall(id, this.service, null, null, null, null,
                        totalRequestSize, null, 0, this.callCleanup);
                readParamsFailedCall.setResponse(null, null, responseThrowable,
                        msg + "; " + responseThrowable.getMessage());
                TraceUtil.setError(span, responseThrowable);
                readParamsFailedCall.sendResponseIfReady();
                return;
            }

            int timeout = 0;
            if (header.hasTimeout() && header.getTimeout() > 0) {
                timeout = Math.max(this.rpcServer.minClientRequestTimeout, header.getTimeout());
            }
            ServerCall<?> call = createCall(id, this.service, md, header, param, cellScanner,
                    totalRequestSize, this.addr, timeout, this.callCleanup);

            if (this.rpcServer.scheduler.dispatch(new CallRunner(this.rpcServer, call))) {
                // unset span do that it's not closed in the finally block
                span = null;
            } else {
                this.rpcServer.callQueueSizeInBytes.add(-1 * call.getSize());
                // this.rpcServer.metrics.exception(RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
                call.setResponse(null, null, RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION,
                        "Call queue is full on " + this.rpcServer.server.getServerName()
                                + ", too many items queued ?");
                TraceUtil.setError(span, RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
                call.sendResponseIfReady();
            }
        } finally {
            if (span != null) {
                span.end();
            }
        }
    }

    // Reads the connection header following version
    private void processConnectionHeader(ByteBuff buf) throws IOException {
        if (buf.hasArray()) {
            this.connectionHeader = ConnectionHeader.parseFrom(buf.array());
        } else {
            CodedInputStream cis = UnsafeByteOperations
                    .unsafeWrap(new ByteBuffByteInput(buf, 0, buf.limit()), 0, buf.limit()).newCodedInput();
            cis.enableAliasing(true);
            this.connectionHeader = ConnectionHeader.parseFrom(cis);
        }
        LOG.debug("ServerRpcConnection.processConnectionHeader {}", this.connectionHeader);
        String serviceName = connectionHeader.getServiceName();
        if (serviceName == null)
            throw new UnknownServiceException("Empty service name");
        this.service = RpcServer.getService(this.rpcServer.services, serviceName);
        if (this.service == null)
            throw new UnknownServiceException(serviceName);
        setupCellBlockCodecs(this.connectionHeader);
        RPCProtos.ConnectionHeaderResponse.Builder chrBuilder = RPCProtos.ConnectionHeaderResponse.newBuilder();
        setupCryptoCipher(this.connectionHeader, chrBuilder);
        responseConnectionHeader(chrBuilder);
    }

    /**
     * Set up cipher for rpc encryption with Apache Commons Crypto
     */
    private void setupCryptoCipher(final ConnectionHeader header,
            RPCProtos.ConnectionHeaderResponse.Builder chrBuilder) throws FatalConnectionException {
        // If simple auth, return
        // check if rpc encryption with Crypto AES
        if (!header.hasRpcCryptoCipherTransformation())
            return;
        String transformation = header.getRpcCryptoCipherTransformation();
        if (transformation == null || transformation.length() == 0)
            return;
        // Negotiates AES based on complete saslServer.
        // The Crypto metadata need to be encrypted and send to client.
        Properties properties = new Properties();
        // the property for SecureRandomFactory
        properties.setProperty(CryptoRandomFactory.CLASSES_KEY,
                this.rpcServer.conf.get("hbase.crypto.sasl.encryption.aes.crypto.random",
                        "org.apache.commons.crypto.random.JavaCryptoRandom"));
        // the property for cipher class
        properties.setProperty(CryptoCipherFactory.CLASSES_KEY,
                this.rpcServer.conf.get("hbase.rpc.crypto.encryption.aes.cipher.class",
                        "org.apache.commons.crypto.cipher.JceCipher"));

        int cipherKeyBits = this.rpcServer.conf.getInt("hbase.rpc.crypto.encryption.aes.cipher.keySizeBits", 128);
        // generate key and iv
        if (cipherKeyBits % 8 != 0) {
            throw new IllegalArgumentException(
                    "The AES cipher key size in bits" + " should be a multiple of byte");
        }
        int len = cipherKeyBits / 8;
        byte[] inKey = new byte[len];
        byte[] outKey = new byte[len];
        byte[] inIv = new byte[len];
        byte[] outIv = new byte[len];

        try {
            // generate the cipher meta data with SecureRandom
            CryptoRandom secureRandom = CryptoRandomFactory.getCryptoRandom(properties);
            secureRandom.nextBytes(inKey);
            secureRandom.nextBytes(outKey);
            secureRandom.nextBytes(inIv);
            secureRandom.nextBytes(outIv);

            // create CryptoAES for server
            cryptoAES = new CryptoAES(transformation, properties, inKey, outKey, inIv, outIv);
            // create SaslCipherMeta and send to client,
            // for client, the [inKey, outKey], [inIv, outIv] should be reversed
            RPCProtos.CryptoCipherMeta.Builder ccmBuilder = RPCProtos.CryptoCipherMeta.newBuilder();
            ccmBuilder.setTransformation(transformation);
            ccmBuilder.setInIv(getByteString(outIv));
            ccmBuilder.setInKey(getByteString(outKey));
            ccmBuilder.setOutIv(getByteString(inIv));
            ccmBuilder.setOutKey(getByteString(inKey));
            chrBuilder.setCryptoCipherMeta(ccmBuilder);
            useCryptoAesWrap = true;
        } catch (Exception ex) {
            throw new UnsupportedCryptoException(ex.getMessage(), ex);
        }
    }

    /**
     * Set up cell block codecs
     */
    private void setupCellBlockCodecs(final ConnectionHeader header) throws FatalConnectionException {
        // TODO: Plug in other supported decoders.
        if (!header.hasCellBlockCodecClass())
            return;
        String className = header.getCellBlockCodecClass();
        if (className == null || className.length() == 0)
            return;
        try {
            this.codec = (Codec) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new UnsupportedCellCodecException(className, e);
        }
        if (!header.hasCellBlockCompressorClass())
            return;
        className = header.getCellBlockCompressorClass();
        try {
            this.compressionCodec = (CompressionCodec) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new UnsupportedCompressionCodecException(className, e);
        }
    }

    private static class ByteBuffByteInput extends ByteInput {

        private ByteBuff buf;
        private int offset;
        private int length;

        ByteBuffByteInput(ByteBuff buf, int offset, int length) {
            this.buf = buf;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public byte read(int offset) {
            return this.buf.get(getAbsoluteOffset(offset));
        }

        private int getAbsoluteOffset(int offset) {
            return this.offset + offset;
        }

        @Override
        public int read(int offset, byte[] out, int outOffset, int len) {
            this.buf.get(getAbsoluteOffset(offset), out, outOffset, len);
            return len;
        }

        @Override
        public int read(int offset, ByteBuffer out) {
            int len = out.remaining();
            this.buf.get(out, getAbsoluteOffset(offset), len);
            return len;
        }

        @Override
        public int size() {
            return this.length;
        }
    }

    private ByteString getByteString(byte[] bytes) {
        // return singleton to reduce object allocation
        return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
    }

    /**
     * Send the response for connection header
     */
    private void responseConnectionHeader(RPCProtos.ConnectionHeaderResponse.Builder chrBuilder)
            throws FatalConnectionException {
        // Response the connection header if Crypto AES is enabled
        if (!chrBuilder.hasCryptoCipherMeta())
            return;
        try {
            byte[] connectionHeaderResBytes = chrBuilder.build().toByteArray();
            // encrypt the Crypto AES cipher meta data with sasl server, and send to client
            byte[] unwrapped = new byte[connectionHeaderResBytes.length + 4];
            Bytes.putBytes(unwrapped, 0, Bytes.toBytes(connectionHeaderResBytes.length), 0, 4);
            Bytes.putBytes(unwrapped, 4, connectionHeaderResBytes, 0, connectionHeaderResBytes.length);
            byte wrapped[] = unwrapped;
            BufferChain bc;
            try (ByteBufferOutputStream response = new ByteBufferOutputStream(wrapped.length + 4);
                    DataOutputStream out = new DataOutputStream(response)) {
                out.writeInt(wrapped.length);
                out.write(wrapped);
                bc = new BufferChain(response.getByteBuffer());
            }
            doRespond(() -> bc);
        } catch (IOException ex) {
            throw new UnsupportedCryptoException(ex.getMessage(), ex);
        }
    }

    protected abstract void doRespond(RpcResponse resp) throws IOException;

    public abstract boolean isConnectionOpen();

    public VersionInfo getVersionInfo() {
        if (connectionHeader.hasVersionInfo()) {
            return connectionHeader.getVersionInfo();
        }
        return null;
    }

    public abstract ServerCall<?> createCall(int id, BlockingService service, MethodDescriptor md,
            RequestHeader header, Message param, CellScanner cellScanner, long size,
            InetAddress remoteAddress, int timeout, CallCleanup reqCleanup);

}
