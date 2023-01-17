package org.waterme7on.hbase.ipc;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.trace.IpcClientSpanBuilder;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.security.User;
// import org.apache.hadoop.hbase.server.trace.IpcServerSpanBuilder;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

public class CallRunner {
    private static final CallDroppedException CALL_DROPPED_EXCEPTION = new CallDroppedException();

    private RpcCall call;
    private RpcServerInterface rpcServer;
    private final Span span;
    private boolean successful = false;

    /**
     * On construction, adds the size of this call to the running count of
     * outstanding call sizes.
     * Presumption is that we are put on a queue while we wait on an executor to run
     * us. During this
     * time we occupy heap.
     */
    // The constructor is shutdown so only RpcServer in this class can make one of
    // these.
    CallRunner(final RpcServerInterface rpcServer, final RpcCall call) {
        this.call = call;
        this.rpcServer = rpcServer;
        this.span = Span.current();
        // Add size of the call to queue size.
        if (call != null && rpcServer != null) {
            this.rpcServer.addCallSize(call.getSize());
        }
    }

    public RpcCall getRpcCall() {
        return call;
    }

    /**
     * Cleanup after ourselves... let go of references.
     */
    private void cleanup() {
        this.call.cleanup();
        this.call = null;
        this.rpcServer = null;
    }

    public void run() {
        try (Scope ignored = span.makeCurrent()) {
            if (call.disconnectSince() >= 0) {
                RpcServer.LOG.debug("{}: skipped {}", Thread.currentThread().getName(), call);
                span.addEvent("Client disconnect detected");
                span.setStatus(StatusCode.OK);
                return;
            }
            call.setStartTime(EnvironmentEdgeManager.currentTime());
            if (call.getStartTime() > call.getDeadline()) {
                RpcServer.LOG.warn("Dropping timed out call: {}", call);
                // this.rpcServer.getMetrics().callTimedOut();
                span.addEvent("Call deadline exceeded");
                span.setStatus(StatusCode.OK);
                return;
            }
            // this.status.setStatus("Setting up call");
            // this.status.setConnection(call.getRemoteAddress().getHostAddress(),
            // call.getRemotePort());
            if (RpcServer.LOG.isTraceEnabled()) {
                RpcServer.LOG.trace("{} executing as {}", call.toShortString(),
                        call.getRequestUser().map(User::getName).orElse("NULL principal"));
            }
            Throwable errorThrowable = null;
            String error = null;
            Pair<Message, CellScanner> resultPair = null;
            RpcServer.CurCall.set(call);
            final Span ipcServerSpan = new IpcClientSpanBuilder().build();
            try (Scope ignored1 = ipcServerSpan.makeCurrent()) {
                if (!this.rpcServer.isStarted()) {
                    InetSocketAddress address = rpcServer.getListenerAddress();
                    throw new ServerNotRunningYetException(
                            "Server " + (address != null ? address : "(channel closed)") + " is not running yet");
                }
                // make the call
                resultPair = this.rpcServer.call(call);
            } catch (TimeoutIOException e) {
                RpcServer.LOG.warn("Can not complete this request in time, drop it: {}", call);
                TraceUtil.setError(ipcServerSpan, e);
                return;
            } catch (Throwable e) {
                TraceUtil.setError(ipcServerSpan, e);
                if (e instanceof ServerNotRunningYetException) {
                    // If ServerNotRunningYetException, don't spew stack trace.
                    if (RpcServer.LOG.isTraceEnabled()) {
                        RpcServer.LOG.trace(call.toShortString(), e);
                    }
                } else {
                    // Don't dump full exception.. just String version
                    RpcServer.LOG.debug("{}, exception={}", call.toShortString(), e);
                }
                errorThrowable = e;
                error = StringUtils.stringifyException(e);
                if (e instanceof Error) {
                    throw (Error) e;
                }
            } finally {
                RpcServer.CurCall.set(null);
                if (resultPair != null) {
                    this.rpcServer.addCallSize(call.getSize() * -1);
                    ipcServerSpan.setStatus(StatusCode.OK);
                    successful = true;
                }
                ipcServerSpan.end();
            }
            // this.status.markComplete("To send response");
            // return the RPC request read BB we can do here. It is done by now.
            call.cleanup();
            // Set the response
            Message param = resultPair != null ? resultPair.getFirst() : null;
            CellScanner cells = resultPair != null ? resultPair.getSecond() : null;
            call.setResponse(param, cells, errorThrowable, error);
            call.sendResponseIfReady();
            // don't touch `span` here because its status and `end()` are managed in
            // `call#setResponse()`
        } catch (OutOfMemoryError e) {
            TraceUtil.setError(span, e);
            throw e;
        } catch (ClosedChannelException cce) {
            InetSocketAddress address = rpcServer.getListenerAddress();
            RpcServer.LOG.warn(
                    "{}: caught a ClosedChannelException, " + "this means that the server "
                            + (address != null ? address : "(channel closed)")
                            + " was processing a request but the client went away. The error message was: {}",
                    Thread.currentThread().getName(), cce.getMessage());
            TraceUtil.setError(span, cce);
        } catch (Exception e) {
            RpcServer.LOG.warn("{}: caught: {}", Thread.currentThread().getName(),
                    StringUtils.stringifyException(e));
            TraceUtil.setError(span, e);
        } finally {
            if (!successful) {
                this.rpcServer.addCallSize(call.getSize() * -1);
            }
            cleanup();
            span.end();
        }
    }

    /**
     * When we want to drop this call because of server is overloaded.
     */
    public void drop() {
        try (Scope ignored = span.makeCurrent()) {
            if (call.disconnectSince() >= 0) {
                RpcServer.LOG.debug("{}: skipped {}", Thread.currentThread().getName(), call);
                span.addEvent("Client disconnect detected");
                span.setStatus(StatusCode.OK);
                return;
            }

            // Set the response
            InetSocketAddress address = rpcServer.getListenerAddress();
            call.setResponse(null, null, CALL_DROPPED_EXCEPTION, "Call dropped, server "
                    + (address != null ? address : "(channel closed)") + " is overloaded, please retry.");
            TraceUtil.setError(span, CALL_DROPPED_EXCEPTION);
            call.sendResponseIfReady();
            // this.rpcServer.getMetrics().exception(CALL_DROPPED_EXCEPTION);
        } catch (ClosedChannelException cce) {
            InetSocketAddress address = rpcServer.getListenerAddress();
            RpcServer.LOG.warn(
                    "{}: caught a ClosedChannelException, " + "this means that the server "
                            + (address != null ? address : "(channel closed)")
                            + " was processing a request but the client went away. The error message was: {}",
                    Thread.currentThread().getName(), cce.getMessage());
            TraceUtil.setError(span, cce);
        } catch (Exception e) {
            RpcServer.LOG.warn("{}: caught: {}", Thread.currentThread().getName(),
                    StringUtils.stringifyException(e));
            TraceUtil.setError(span, e);
        } finally {
            if (!successful) {
                this.rpcServer.addCallSize(call.getSize() * -1);
            }
            cleanup();
            span.end();
        }
    }
}
