package org.waterme7on.hbase.ipc;

import java.io.IOException;
// import java.util.Optional;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import io.opentelemetry.api.trace.Span;

import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;
// import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

class Call {
    final int id; // call id
    final Message param; // rpc request method param object
    Message response; // value, null if error
    /**
     * Optionally has cells when making call. Optionally has cells set on response.
     * Used passing cells
     * to the rpc and receiving the response.
     */
    CellScanner cells;
    final int priority;
    // The return type. Used to create shell into which we deserialize the response
    // if any.
    Message responseDefaultType;
    IOException error; // exception, null if value
    private boolean done; // true when call is done
    final int timeout; // timeout in millisecond for this call; 0 means infinite.
    final MetricsConnection.CallStats callStats;
    final Descriptors.MethodDescriptor md;
    private final RpcCallback<Call> callback;
    final Span span;
    Timeout timeoutTask;

    Call(int id, final Descriptors.MethodDescriptor md, Message param, final CellScanner cells,
            final Message responseDefaultType, int timeout, int priority, RpcCallback<Call> callback,
            MetricsConnection.CallStats callStats) {
        this.param = param;
        this.md = md;
        this.cells = cells;
        this.callStats = callStats;
        this.callStats.setStartTime(EnvironmentEdgeManager.currentTime());
        this.responseDefaultType = responseDefaultType;
        this.id = id;
        this.timeout = timeout;
        this.priority = priority;
        this.callback = callback;
        this.span = Span.current();
    }

    /**
     * Builds a simplified {@link #toString()} that includes just the id and method
     * name.
     */
    public String toShortString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", id)
                .append("methodName", md.getName()).toString();
    }

    @Override
    public String toString() {
        // Call[id=32153218,methodName=Get]
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).appendSuper(toShortString()).toString();
    }

    /**
     * called from timeoutTask, prevent self cancel
     */
    public void setTimeout(IOException error) {
        synchronized (this) {
            if (done) {
                return;
            }
            this.done = true;
            this.error = error;
        }
        callback.run(this);
    }

    private void callComplete() {
        if (timeoutTask != null) {
            timeoutTask.cancel();
        }
        callback.run(this);
    }

    /**
     * Set the exception when there is an error. Notify the caller the call is done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public void setException(IOException error) {
        synchronized (this) {
            if (done) {
                return;
            }
            this.done = true;
            this.error = error;
        }
        callComplete();
    }

    /**
     * Set the return value when there is no error. Notify the caller the call is
     * done.
     * 
     * @param response return value of the call.
     * @param cells    Can be null
     */
    public void setResponse(Message response, final CellScanner cells) {
        synchronized (this) {
            if (done) {
                return;
            }
            this.done = true;
            this.response = response;
            this.cells = cells;
        }
        callComplete();
    }

    public synchronized boolean isDone() {
        return done;
    }

    public long getStartTime() {
        return this.callStats.getStartTime();
    }
}
