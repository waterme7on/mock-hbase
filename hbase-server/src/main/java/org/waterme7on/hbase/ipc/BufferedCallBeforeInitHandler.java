package org.waterme7on.hbase.ipc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.channel.ChannelDuplexHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;

/**
 * We will expose the connection to upper layer before initialized, so we need
 * to buffer the calls
 * passed in and write them out once the connection is established.
 */
@InterfaceAudience.Private
class BufferCallBeforeInitHandler extends ChannelDuplexHandler {

    static final String NAME = "BufferCall";

    private enum BufferCallAction {
        FLUSH,
        FAIL
    }

    public static final class BufferCallEvent {

        public final BufferCallAction action;

        public final IOException error;

        private BufferCallEvent(BufferCallBeforeInitHandler.BufferCallAction action,
                IOException error) {
            this.action = action;
            this.error = error;
        }

        public static BufferCallBeforeInitHandler.BufferCallEvent success() {
            return SUCCESS_EVENT;
        }

        public static BufferCallBeforeInitHandler.BufferCallEvent fail(IOException error) {
            return new BufferCallEvent(BufferCallAction.FAIL, error);
        }
    }

    private static final BufferCallEvent SUCCESS_EVENT = new BufferCallEvent(BufferCallAction.FLUSH, null);

    private final Map<Integer, Call> id2Call = new HashMap<>();

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof Call) {
            Call call = (Call) msg;
            id2Call.put(call.id, call);
            // The call is already in track so here we set the write operation as success.
            // We will fail the call directly if we can not write it out.
            promise.trySuccess();
        } else {
            NettyFutureUtils.consume(ctx.write(msg, promise));
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        // do not flush anything out
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof BufferCallEvent) {
            BufferCallEvent bcEvt = (BufferCallBeforeInitHandler.BufferCallEvent) evt;
            switch (bcEvt.action) {
                case FLUSH:
                    for (Call call : id2Call.values()) {
                        NettyFutureUtils.safeWrite(ctx, call);
                    }
                    break;
                case FAIL:
                    for (Call call : id2Call.values()) {
                        call.setException(bcEvt.error);
                    }
                    break;
            }
            ctx.flush();
            ctx.pipeline().remove(this);
        } else if (evt instanceof CallEvent) {
            // just remove the call for now until we add other call event other than timeout
            // and cancel.
            id2Call.remove(((CallEvent) evt).call.id);
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }
}
