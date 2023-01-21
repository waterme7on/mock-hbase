package org.waterme7on.hbase;

/**
 * This is a special {@link ScannerContext} subclass that is designed to be used
 * globally when
 * limits should not be enforced during invocations of
 * {@link InternalScanner#next(java.util.List)}
 * or {@link RegionScanner#next(java.util.List)}.
 * <p>
 * Instances of {@link NoLimitScannerContext} are immutable after construction.
 * Any attempt to
 * change the limits or progress of a {@link NoLimitScannerContext} will fail
 * silently. The net
 * effect is that all limit checks will return false, thus indicating that a
 * limit has not been
 * reached.
 */
public class NoLimitScannerContext extends ScannerContext {

    public NoLimitScannerContext() {
        super(false, null, false);
    }

    /**
     * Use this instance whenever limits do not need to be enforced.
     */
    private static final ScannerContext NO_LIMIT = new NoLimitScannerContext();

    /**
     * Returns the static, immutable instance of {@link NoLimitScannerContext} to be
     * used whenever
     * limits should not be enforced
     */
    @SuppressWarnings("MS_EXPOSE_REP")
    public static final ScannerContext getInstance() {
        return NO_LIMIT;
    }

    @Override
    public void setKeepProgress(boolean keepProgress) {
        // Do nothing. NoLimitScannerContext instances are immutable post-construction
    }

    @Override
    public void setBatchProgress(int batchProgress) {
        // Do nothing. NoLimitScannerContext instances are immutable post-construction
    }

    @Override
    public void setSizeProgress(long sizeProgress, long heapSizeProgress) {
        // Do nothing. NoLimitScannerContext instances are immutable post-construction
    }

    @Override
    public void setProgress(int batchProgress, long sizeProgress, long heapSizeProgress) {
        // Do nothing. NoLimitScannerContext instances are immutable post-construction
    }

    @Override
    public void clearProgress() {
        // Do nothing. NoLimitScannerContext instances are immutable post-construction
    }

    @Override
    public void setSizeLimitScope(LimitScope scope) {
        // Do nothing. NoLimitScannerContext instances are immutable post-construction
    }

    @Override
    public void setTimeLimitScope(LimitScope scope) {
        // Do nothing. NoLimitScannerContext instances are immutable post-construction
    }

    @Override
    public NextState setScannerState(NextState state) {
        // Do nothing. NoLimitScannerContext instances are immutable post-construction
        return state;
    }

    @Override
    public boolean checkBatchLimit(LimitScope checkerScope) {
        // No limits can be specified, thus return false to indicate no limit has been
        // reached.
        return false;
    }

    @Override
    public boolean checkSizeLimit(LimitScope checkerScope) {
        // No limits can be specified, thus return false to indicate no limit has been
        // reached.
        return false;
    }

    @Override
    public boolean checkTimeLimit(LimitScope checkerScope) {
        // No limits can be specified, thus return false to indicate no limit has been
        // reached.
        return false;
    }

    @Override
    public boolean checkAnyLimitReached(LimitScope checkerScope) {
        return false;
    }
}
