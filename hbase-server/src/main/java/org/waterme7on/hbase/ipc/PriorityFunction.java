package org.waterme7on.hbase.ipc;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.User;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * Function to figure priority of incoming request.
 */
public interface PriorityFunction {
    /**
     * Returns the 'priority type' of the specified request. The returned value is
     * mainly used to
     * select the dispatch queue.
     * 
     * @return Priority of this request.
     */
    int getPriority(RequestHeader header, Message param, User user);

    /**
     * Returns the deadline of the specified request. The returned value is used to
     * sort the dispatch
     * queue.
     * 
     * @return Deadline of this request. 0 now, otherwise msec of 'delay'
     */
    long getDeadline(RequestHeader header, Message param);
}
