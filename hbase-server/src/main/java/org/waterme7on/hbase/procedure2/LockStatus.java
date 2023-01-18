package org.waterme7on.hbase.procedure2;

/**
 * Interface to get status of a Lock without getting access to acquire/release
 * lock. Currently used
 * in MasterProcedureScheduler where we want to give Queues access to lock's
 * status for scheduling
 * purposes, but not the ability to acquire/release it.
 */
public interface LockStatus {

    /**
     * Return whether this lock has already been held,
     * <p/>
     * Notice that, holding the exclusive lock or shared lock are both considered as
     * locked, i.e, this
     * method usually equals to
     * {@code hasExclusiveLock() || getSharedLockCount() > 0}.
     */
    default boolean isLocked() {
        return hasExclusiveLock() || getSharedLockCount() > 0;
    }

    /**
     * Whether the exclusive lock has been held.
     */
    boolean hasExclusiveLock();

    /**
     * Return true if the procedure itself holds the exclusive lock, or any
     * ancestors of the give
     * procedure hold the exclusive lock.
     */
    boolean hasLockAccess(Procedure<?> proc);

    /**
     * Get the procedure which holds the exclusive lock.
     */
    Procedure<?> getExclusiveLockOwnerProcedure();

    /**
     * Return the id of the procedure which holds the exclusive lock, if exists. Or
     * a negative value
     * which means no one holds the exclusive lock.
     * <p/>
     * Notice that, in HBase, we assume that the procedure id is positive, or at
     * least non-negative.
     */
    default long getExclusiveLockProcIdOwner() {
        Procedure<?> proc = getExclusiveLockOwnerProcedure();
        return proc != null ? proc.getProcId() : -1L;
    }

    /**
     * Get the number of procedures which hold the shared lock.
     */
    int getSharedLockCount();
}
