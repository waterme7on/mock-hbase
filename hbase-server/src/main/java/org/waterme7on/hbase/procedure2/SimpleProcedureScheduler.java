package org.waterme7on.hbase.procedure2;

import java.util.Collections;
import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Simple scheduler for procedures
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SimpleProcedureScheduler extends AbstractProcedureScheduler {
    private final ProcedureDeque runnables = new ProcedureDeque();

    @Override
    protected void enqueue(final Procedure procedure, final boolean addFront) {
        if (addFront) {
            runnables.addFirst(procedure);
        } else {
            runnables.addLast(procedure);
        }
    }

    @Override
    protected Procedure dequeue() {
        return runnables.poll();
    }

    @Override
    public void clear() {
        schedLock();
        try {
            runnables.clear();
        } finally {
            schedUnlock();
        }
    }

    @Override
    public void yield(final Procedure proc) {
        addBack(proc);
    }

    @Override
    public boolean queueHasRunnables() {
        return runnables.size() > 0;
    }

    @Override
    public int queueSize() {
        return runnables.size();
    }

    @Override
    public void completionCleanup(Procedure proc) {
    }

    @Override
    public List<LockedResource> getLocks() {
        return Collections.emptyList();
    }

    @Override
    public LockedResource getLockResource(LockedResourceType resourceType, String resourceName) {
        return null;
    }
}
