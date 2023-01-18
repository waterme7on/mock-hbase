package org.waterme7on.hbase.procedure2;

import java.util.List;

public class LockedResource {
    private final LockedResourceType resourceType;
    private final String resourceName;
    private final LockType lockType;
    private final Procedure<?> exclusiveLockOwnerProcedure;
    private final int sharedLockCount;
    private final List<Procedure<?>> waitingProcedures;

    public LockedResource(LockedResourceType resourceType, String resourceName, LockType lockType,
            Procedure<?> exclusiveLockOwnerProcedure, int sharedLockCount,
            List<Procedure<?>> waitingProcedures) {
        this.resourceType = resourceType;
        this.resourceName = resourceName;
        this.lockType = lockType;
        this.exclusiveLockOwnerProcedure = exclusiveLockOwnerProcedure;
        this.sharedLockCount = sharedLockCount;
        this.waitingProcedures = waitingProcedures;
    }

    public LockedResourceType getResourceType() {
        return resourceType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public LockType getLockType() {
        return lockType;
    }

    public Procedure<?> getExclusiveLockOwnerProcedure() {
        return exclusiveLockOwnerProcedure;
    }

    public int getSharedLockCount() {
        return sharedLockCount;
    }

    public List<Procedure<?>> getWaitingProcedures() {
        return waitingProcedures;
    }
}
