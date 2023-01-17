package org.waterme7on.hbase.ipc;

public interface QueueBalancer {
  /** Returns the index of the next queue to which a request should be inserted */
  int getNextQueue(CallRunner callRunner);
}
