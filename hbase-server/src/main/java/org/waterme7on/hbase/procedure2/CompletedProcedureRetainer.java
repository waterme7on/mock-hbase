package org.waterme7on.hbase.procedure2;

/**
 * Hold the reference to a completed root procedure. Will be cleaned up after
 * expired.
 */
class CompletedProcedureRetainer<TEnvironment> {
    private final Procedure<TEnvironment> procedure;
    private long clientAckTime;

    public CompletedProcedureRetainer(Procedure<TEnvironment> procedure) {
        this.procedure = procedure;
        clientAckTime = -1;
    }

    public Procedure<TEnvironment> getProcedure() {
        return procedure;
    }

    public boolean hasClientAckTime() {
        return clientAckTime != -1;
    }

    public long getClientAckTime() {
        return clientAckTime;
    }

    public void setClientAckTime(long clientAckTime) {
        this.clientAckTime = clientAckTime;
    }

    public boolean isExpired(long now, long evictTtl, long evictAckTtl) {
        return (hasClientAckTime() && (now - getClientAckTime()) >= evictAckTtl)
                || (now - procedure.getLastUpdate()) >= evictTtl;
    }
}
