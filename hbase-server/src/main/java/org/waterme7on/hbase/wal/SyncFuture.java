package org.waterme7on.hbase.wal;

public class SyncFuture {

    private long txid;

    public void get(long walSyncTimeoutNs) {
    }

    public void setTxid(long txid) {
        this.txid = txid;
    }

    long getTxid() {
        return this.txid;
    }

    public void done(long txid2, Throwable t) {
    }

    public SyncFuture reset(long l, boolean b) {
        return null;
    }

    public boolean isForceSync() {
        return false;
    }

}
