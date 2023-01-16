package org.waterme7on.hbase.ipc;

import java.net.InetAddress;

public interface RpcCallContext {
    /** Returns Address of remote client in this call */
    InetAddress getRemoteAddress();
}
