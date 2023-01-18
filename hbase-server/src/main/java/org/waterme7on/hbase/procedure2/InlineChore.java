package org.waterme7on.hbase.procedure2;

import org.waterme7on.hbase.coprocessor.DelayUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Inline Chores (executors internal chores).
 */
@InterfaceAudience.Private
abstract class InlineChore extends DelayUtil.DelayedObject implements Runnable {

    private long timeout;

    public abstract int getTimeoutInterval();

    protected void refreshTimeout() {
        this.timeout = EnvironmentEdgeManager.currentTime() + getTimeoutInterval();
    }

    @Override
    public long getTimeout() {
        return timeout;
    }
}
