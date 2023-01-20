package org.waterme7on.hbase.regionserver;

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.REGION_NAMES_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.ROW_LOCK_READ_LOCK_KEY;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.*;

import io.opentelemetry.api.trace.Span;
import org.waterme7on.hbase.wal.WALEdit;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.PrivateCellUtil;
import org.waterme7on.hbase.ipc.RpcCall;
import org.waterme7on.hbase.ipc.RpcServer;
import org.waterme7on.hbase.monitoring.MonitoredTask;
import org.waterme7on.hbase.monitoring.TaskMonitor;
import org.waterme7on.hbase.util.ClassSize;
import org.waterme7on.hbase.util.ServerRegionReplicaUtil;
import org.waterme7on.hbase.wal.WAL;

/*
*  * <pre>
* hbase
*   |
*   --region dir
*       |
*       --data
*       |  |
*       |  --ns/table/encoded-region-name <---- The region data
*       |      |
*       |      --replay <---- The edits to replay
*       |
*       --WALs
*          |
*          --server-name <---- The WAL dir
* */
public class HRegion implements Region {
    private static final Logger LOG = LoggerFactory.getLogger(HRegion.class);

    public static final String HBASE_MAX_CELL_SIZE_KEY = "hbase.server.keyvalue.maxsize";
    public static final int DEFAULT_MAX_CELL_SIZE = 10485760;
    // Number of mutations for minibatch processing.
    public static final String HBASE_REGIONSERVER_MINIBATCH_SIZE = "hbase.regionserver.minibatch.size";
    public static final int DEFAULT_HBASE_REGIONSERVER_MINIBATCH_SIZE = 20000;
    private final int miniBatchSize;

    // Used to guard closes
    final ReentrantReadWriteLock lock;
    public static final String FAIR_REENTRANT_CLOSE_LOCK = "hbase.regionserver.fair.region.close.lock";
    public static final boolean DEFAULT_FAIR_REENTRANT_CLOSE_LOCK = true;
    /** Conf key for the periodic flush interval */

    // If updating multiple rows in one call, wait longer,
    // i.e. waiting for busyWaitDuration * # of rows. However,
    // we can limit the max multiplier.
    // The internal wait duration to acquire a lock before read/update
    // from the region. It is not per row. The purpose of this wait time
    // is to avoid waiting a long time while the region is busy, so that
    // we can release the IPC handler soon enough to improve the
    // availability of the region server. It can be adjusted by
    // tuning configuration "hbase.busy.wait.duration".
    final long busyWaitDuration;
    static final long DEFAULT_BUSY_WAIT_DURATION = HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
    final int maxBusyWaitMultiplier;

    // Max busy wait duration. There is no point to wait longer than the RPC
    // purge timeout, when a RPC call will be terminated by the RPC engine.
    final long maxBusyWaitDuration;

    final long maxCellSize;
    long memstoreFlushSize;
    // map from a locked row to the context for that lock including:
    // - CountDownLatch for threads waiting on that row
    // - the thread that owns the lock (allow reentrancy)
    // - reference count of (reentrant) locks held by the thread
    // - the row itself
    private final ConcurrentHashMap<HashedBytes, RowLockContext> lockedRows = new ConcurrentHashMap<>();
    protected final Map<byte[], HStore> stores = new ConcurrentSkipListMap<>(Bytes.BYTES_RAWCOMPARATOR);
    protected RegionServerServices rsServices;

    // Track data size in all memstores
    private final MemStoreSizing memStoreSizing = new ThreadSafeMemStoreSizing();
    // Number of requests blocked by memstore size.
    private final LongAdder blockedRequestsCount = new LongAdder();
    private long flushSize;
    private long flushIntervalMs;
    private final CellComparator cellComparator;
    private TableDescriptor htableDescriptor = null;
    // Stop updates lock
    private final ReentrantReadWriteLock updatesLock = new ReentrantReadWriteLock();

    // Context: During replay we want to ensure that we do not lose any data. So, we
    // have to be conservative in how we replay wals. For each store, we calculate
    // the maxSeqId up to which the store was flushed. And, skip the edits which
    // are equal to or lower than maxSeqId for each store.
    // The following map is populated when opening the region
    Map<byte[], Long> maxSeqIdInStores = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    // Used to track interruptible holders of the region lock. Currently that is
    // only RPC handler
    // threads. Boolean value in map determines if lock holder can be interrupted,
    // normally true,
    // but may be false when thread is transiting a critical section.
    final ConcurrentHashMap<Thread, Boolean> regionLockHolders;

    private long blockingMemStoreSize;
    final AtomicBoolean closed = new AtomicBoolean(false);
    // set to true if the region is restored from snapshot
    private boolean isRestoredRegion = false;
    private final WAL wal;
    // Last flush time for each Store. Useful when we are flushing for each column
    private final ConcurrentMap<HStore, Long> lastStoreFlushTimeMap = new ConcurrentHashMap<>();
    /*
     * Closing can take some time; use the closing flag if there is stuff we don't
     * want to do while in
     * closing state; e.g. like offer this region up to the master as a region to
     * close if the
     * carrying regionserver is overloaded. Once set, it is never cleared.
     */
    final AtomicBoolean closing = new AtomicBoolean(false);

    private final HRegionFileSystem fs;
    private final Configuration conf;
    private final int rowLockWaitDuration;
    static final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;
    final WriteState writestate = new WriteState();
    protected volatile long lastReplayedOpenRegionSeqId = -1L;

    private long openSeqNum;

    public static final String USE_META_CELL_COMPARATOR = "hbase.region.use.meta.cell.comparator";
    public static final boolean DEFAULT_USE_META_CELL_COMPARATOR = false;

    // TODO
    public HRegion(final Path tableDir, final WAL wal, final FileSystem fs,
            final Configuration conf, final RegionInfo regionInfo,
            final TableDescriptor htd,
            final RegionServerServices rsServices) {
        this.cellComparator = htd.isMetaTable()
                || conf.getBoolean(USE_META_CELL_COMPARATOR, DEFAULT_USE_META_CELL_COMPARATOR)
                        ? MetaCellComparator.META_COMPARATOR
                        : CellComparatorImpl.COMPARATOR;
        this.lock = new ReentrantReadWriteLock(
                conf.getBoolean(FAIR_REENTRANT_CLOSE_LOCK, DEFAULT_FAIR_REENTRANT_CLOSE_LOCK));

        this.conf = conf;
        this.wal = wal;
        this.fs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
        this.maxCellSize = conf.getLong(HBASE_MAX_CELL_SIZE_KEY, DEFAULT_MAX_CELL_SIZE);
        this.htableDescriptor = htd;
        this.rsServices = rsServices;
        this.miniBatchSize = conf.getInt(HBASE_REGIONSERVER_MINIBATCH_SIZE, DEFAULT_HBASE_REGIONSERVER_MINIBATCH_SIZE);
        this.busyWaitDuration = conf.getLong("hbase.busy.wait.duration", DEFAULT_BUSY_WAIT_DURATION);
        this.maxBusyWaitMultiplier = conf.getInt("hbase.busy.wait.multiplier.max", 2);
        if (busyWaitDuration * maxBusyWaitMultiplier <= 0L) {
            throw new IllegalArgumentException("Invalid hbase.busy.wait.duration (" + busyWaitDuration
                    + ") or hbase.busy.wait.multiplier.max (" + maxBusyWaitMultiplier
                    + "). Their product should be positive");
        }
        this.maxBusyWaitDuration = conf.getLong("hbase.ipc.client.call.purge.timeout",
                2 * HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
        this.regionLockHolders = new ConcurrentHashMap<>();
        setHTableSpecificConf();
        int tmpRowLockDuration = conf.getInt("hbase.rowlock.wait.duration", DEFAULT_ROWLOCK_WAIT_DURATION);
        if (tmpRowLockDuration <= 0) {
            LOG.info("Found hbase.rowlock.wait.duration set to {}. values <= 0 will cause all row "
                    + "locking to fail. Treating it as 1ms to avoid region failure.", tmpRowLockDuration);
            tmpRowLockDuration = 1;
        }
        this.rowLockWaitDuration = tmpRowLockDuration;

    }

    HRegionFileSystem getRegionFileSystem() {
        return this.fs;
    }

    private void setHTableSpecificConf() {
        if (this.htableDescriptor == null) {
            return;
        }
        long flushSize = this.htableDescriptor.getMemStoreFlushSize();

        if (flushSize <= 0) {
            flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                    TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
        }
        this.memstoreFlushSize = flushSize;
        long mult = conf.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
                HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);
        this.blockingMemStoreSize = this.memstoreFlushSize * mult;
    }

    public static boolean rowIsInRange(RegionInfo info, final byte[] row, final int offset,
            final short length) {
        return ((info.getStartKey().length == 0)
                || (Bytes.compareTo(info.getStartKey(), 0, info.getStartKey().length, row, offset, length) <= 0))
                && ((info.getEndKey().length == 0)
                        || (Bytes.compareTo(info.getEndKey(), 0, info.getEndKey().length, row, offset, length) > 0));
    }

    /**
     * Returns Instance of {@link RegionServerServices} used by this HRegion. Can be
     * null.
     */
    RegionServerServices getRegionServerServices() {
        return this.rsServices;
    }

    @Override
    public RegionInfo getRegionInfo() {
        return this.fs.getRegionInfo();
    }

    @Override
    public void onConfigurationChange(Configuration conf) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onConfigurationChange'");
    }

    @Override
    public TableDescriptor getTableDescriptor() {
        return this.htableDescriptor;
    }

    @Override
    public boolean isAvailable() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isAvailable'");
    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isClosed'");
    }

    @Override
    public boolean isClosing() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isClosing'");
    }

    @Override
    public boolean isReadOnly() {
        // TODO
        return false;
    }

    @Override
    public boolean isSplittable() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isSplittable'");
    }

    @Override
    public boolean isMergeable() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isMergeable'");
    }

    @Override
    public List<? extends Store> getStores() {
        return new ArrayList<>(stores.values());
    }

    @Override
    public Store getStore(byte[] family) {
        return this.stores.get(family);
    }

    @Override
    public List<String> getStoreFileList(byte[][] columns) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStoreFileList'");
    }

    @Override
    public boolean refreshStoreFiles() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'refreshStoreFiles'");
    }

    @Override
    public long getMaxFlushedSeqId() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMaxFlushedSeqId'");
    }

    @Override
    public long getOldestHfileTs(boolean majorCompactionOnly) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOldestHfileTs'");
    }

    @Override
    public Map<byte[], Long> getMaxStoreSeqId() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMaxStoreSeqId'");
    }

    @Override
    public long getEarliestFlushTimeForAllStores() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEarliestFlushTimeForAllStores'");
    }

    @Override
    public long getReadRequestsCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReadRequestsCount'");
    }

    @Override
    public long getFilteredReadRequestsCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFilteredReadRequestsCount'");
    }

    @Override
    public long getWriteRequestsCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getWriteRequestsCount'");
    }

    @Override
    public long getMemStoreDataSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMemStoreDataSize'");
    }

    @Override
    public long getMemStoreHeapSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMemStoreHeapSize'");
    }

    @Override
    public long getMemStoreOffHeapSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMemStoreOffHeapSize'");
    }

    @Override
    public long getNumMutationsWithoutWAL() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getNumMutationsWithoutWAL'");
    }

    @Override
    public long getDataInMemoryWithoutWAL() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDataInMemoryWithoutWAL'");
    }

    @Override
    public long getBlockedRequestsCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBlockedRequestsCount'");
    }

    @Override
    public long getCheckAndMutateChecksPassed() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCheckAndMutateChecksPassed'");
    }

    @Override
    public long getCheckAndMutateChecksFailed() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCheckAndMutateChecksFailed'");
    }

    @Override
    public void startRegionOperation() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'startRegionOperation'");
    }

    private void checkReadsEnabled() throws IOException {
        if (!this.writestate.readsEnabled) {
            throw new IOException(getRegionInfo().getEncodedName()
                    + ": The region's reads are disabled. Cannot serve the request");
        }
    }

    @Override
    public void startRegionOperation(Operation op) throws IOException {
        boolean isInterruptableOp = false;
        switch (op) {
            case GET: // interruptible read operations
            case SCAN:
                isInterruptableOp = true;
                checkReadsEnabled();
                break;
            case INCREMENT: // interruptible write operations
            case APPEND:
            case PUT:
            case DELETE:
            case BATCH_MUTATE:
            case CHECK_AND_MUTATE:
                isInterruptableOp = true;
                break;
            default: // all others
                break;
        }
        if (op == Operation.MERGE_REGION || op == Operation.SPLIT_REGION || op == Operation.COMPACT_REGION
                || op == Operation.COMPACT_SWITCH) {
            // split, merge or compact region doesn't need to check the closing/closed state
            // or lock the
            // region
            return;
        }
        if (this.closing.get()) {
            throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closing");
        }
        lock(lock.readLock());
        // Update regionLockHolders ONLY for any startRegionOperation call that is
        // invoked from
        // an RPC handler
        Thread thisThread = Thread.currentThread();
        if (isInterruptableOp) {
            regionLockHolders.put(thisThread, true);
        }
        if (this.closed.get()) {
            lock.readLock().unlock();
            throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closed");
        }
        // The unit for snapshot is a region. So, all stores for this region must be
        // prepared for snapshot operation before proceeding.
        if (op == Operation.SNAPSHOT) {
            // stores.values().forEach(org.apache.hadoop.hbase.regionserver.HStore::preSnapshotOperation);
        }
        try {
        } catch (Exception e) {
            if (isInterruptableOp) {
                // would be harmless to remove what we didn't add but we know by
                // 'isInterruptableOp'
                // if we added this thread to regionLockHolders
                regionLockHolders.remove(thisThread);
            }
            lock.readLock().unlock();
            throw new IOException(e);
        }
    }

    @Override
    public void closeRegionOperation() throws IOException {
        closeRegionOperation(Operation.ANY);
    }

    @Override
    public void closeRegionOperation(Operation operation) throws IOException {
        if (operation == Operation.SNAPSHOT) {
            // stores.values().forEach(org.apache.hadoop.hbase.regionserver.HStore::postSnapshotOperation);
        }
        Thread thisThread = Thread.currentThread();
        regionLockHolders.remove(thisThread);
        lock.readLock().unlock();
    }

    @Override
    public RowLock getRowLock(byte[] row, boolean readLock) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRowLock'");
    }

    private RowLock getRowLock(byte[] row, boolean readLock, final RowLock prevRowLock)
            throws IOException {
        return TraceUtil.trace(() -> getRowLockInternal(row, readLock, prevRowLock),
                () -> createRegionSpan("Region.getRowLock").setAttribute(ROW_LOCK_READ_LOCK_KEY, readLock));
    }

    public static HRegion openHRegion(RegionInfo regionInfo, TableDescriptor htd, WAL wal, Configuration conf,
            RegionServerServices rsServices) throws IOException {
        return openHRegion(CommonFSUtils.getRootDir(conf), regionInfo, htd, wal, conf, rsServices);
    }

    public static HRegion openHRegion(final Path rootDir, final RegionInfo info,
            final TableDescriptor htd, final WAL wal, final Configuration conf,
            final RegionServerServices rsServices)
            throws IOException {
        FileSystem fs = null;
        if (rsServices != null) {
            fs = rsServices.getFileSystem();
        }
        if (fs == null) {
            fs = rootDir.getFileSystem(conf);
        }
        Path tableDir = CommonFSUtils.getTableDir(rootDir, info.getTable());
        LOG.debug("Opening region: {} from tableDir: {}", info, tableDir);
        return openHRegionFromTableDir(conf, fs, tableDir, info, htd, wal, rsServices);
    }

    public static HRegion openHRegionFromTableDir(final Configuration conf, final FileSystem fs,
            final Path tableDir, final RegionInfo info, final TableDescriptor htd, final WAL wal,
            final RegionServerServices rsServices)
            throws IOException {
        Objects.requireNonNull(info, "RegionInfo cannot be null");
        HRegion r = HRegion.newHRegion(tableDir, wal, fs, conf, info, htd, rsServices);
        return r.openHRegion();
    }

    private HRegion openHRegion() throws IOException {
        this.openSeqNum = initialize(null);
        // The openSeqNum must be increased every time when a region is assigned, as we
        // rely on it to
        // determine whether a region has been successfully reopened. So here we always
        // write open
        // marker, even if the table is read only.
        try {
            if (wal != null && getRegionServerServices() != null
                    && RegionReplicaUtil.isDefaultReplica(getRegionInfo())) {
                // writeRegionOpenMarker(wal, openSeqNum);
                ////////////////////////////////////////
            }
        } catch (Throwable t) {
            // By coprocessor path wrong region will open failed,
            // MetricsRegionWrapperImpl is already init and not close,
            // add region close when open failed
            try {
                // It is not required to write sequence id file when region open is failed.
                // Passing true to skip the sequence id file write.
                this.close();
            } catch (Throwable e) {
                LOG.warn("Open region: {} failed. Try close region but got exception ",
                        this.getRegionInfo(), e);
            }
            throw t;
        }
        return this;
    }

    // will be override in tests
    protected RowLock getRowLockInternal(byte[] row, boolean readLock, RowLock prevRowLock)
            throws IOException {
        // create an object to use a a key in the row lock map
        HashedBytes rowKey = new HashedBytes(row);

        RowLockContext rowLockContext = null;
        RowLockImpl result = null;

        boolean success = false;
        try {
            // Keep trying until we have a lock or error out.
            // TODO: do we need to add a time component here?
            while (result == null) {
                rowLockContext = computeIfAbsent(lockedRows, rowKey, () -> new RowLockContext(rowKey));
                // Now try an get the lock.
                // This can fail as
                if (readLock) {
                    // For read lock, if the caller has locked the same row previously, it will not
                    // try
                    // to acquire the same read lock. It simply returns the previous row lock.
                    RowLockImpl prevRowLockImpl = (RowLockImpl) prevRowLock;
                    if ((prevRowLockImpl != null)
                            && (prevRowLockImpl.getLock() == rowLockContext.readWriteLock.readLock())) {
                        success = true;
                        return prevRowLock;
                    }
                    result = rowLockContext.newReadLock();
                } else {
                    result = rowLockContext.newWriteLock();
                }
            }

            int timeout = rowLockWaitDuration;
            boolean reachDeadlineFirst = false;
            Optional<RpcCall> call = RpcServer.getCurrentCall();
            if (call.isPresent()) {
                long deadline = call.get().getDeadline();
                if (deadline < Long.MAX_VALUE) {
                    int timeToDeadline = (int) (deadline - EnvironmentEdgeManager.currentTime());
                    if (timeToDeadline <= this.rowLockWaitDuration) {
                        reachDeadlineFirst = true;
                        timeout = timeToDeadline;
                    }
                }
            }

            if (timeout <= 0 || !result.getLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
                String message = "Timed out waiting for lock for row: " + rowKey + " in region "
                        + getRegionInfo().getEncodedName();
                if (reachDeadlineFirst) {
                    throw new TimeoutIOException(message);
                } else {
                    // If timeToDeadline is larger than rowLockWaitDuration, we can not drop the
                    // request.
                    throw new IOException(message);
                }
            }
            rowLockContext.setThreadName(Thread.currentThread().getName());
            success = true;
            return result;
        } catch (InterruptedException ie) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Thread interrupted waiting for lock on row: {}, in region {}", rowKey,
                        getRegionInfo().getRegionNameAsString());
            }
            throw throwOnInterrupt(ie);
        } catch (Error error) {
            // The maximum lock count for read lock is 64K (hardcoded), when this maximum
            // count
            // is reached, it will throw out an Error. This Error needs to be caught so it
            // can
            // go ahead to process the minibatch with lock acquired.
            LOG.warn("Error to get row lock for {}, in region {}, cause: {}", Bytes.toStringBinary(row),
                    getRegionInfo().getRegionNameAsString(), error);
            IOException ioe = new IOException(error);
            throw ioe;
        } finally {
            // Clean up the counts just in case this was the thing keeping the context
            // alive.
            if (!success && rowLockContext != null) {
                rowLockContext.cleanUp();
            }
        }
    }

    @Override
    public Result append(Append append) throws IOException {
        return this.append(append, HConstants.NO_NONCE, HConstants.NO_NONCE);
    }

    public Result append(Append append, long nonceGroup, long nonce) throws IOException {
        return TraceUtil.trace(() -> {
            checkReadOnly();
            checkResources();
            startRegionOperation(Operation.APPEND);
            try {
                // All edits for the given row (across all column families) must happen
                // atomically.
                return mutate(append, true, nonceGroup, nonce).getResult();
            } finally {
                closeRegionOperation(Operation.APPEND);
            }
        }, () -> createRegionSpan("Region.append"));
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            ByteArrayComparable comparator, TimeRange timeRange, Mutation mutation) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndMutate'");
    }

    @Override
    public boolean checkAndMutate(byte[] row, Filter filter, TimeRange timeRange, Mutation mutation)
            throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndMutate'");
    }

    @Override
    public boolean checkAndRowMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            ByteArrayComparable comparator, TimeRange timeRange, RowMutations mutations) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndRowMutate'");
    }

    @Override
    public boolean checkAndRowMutate(byte[] row, Filter filter, TimeRange timeRange, RowMutations mutations)
            throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndRowMutate'");
    }

    @Override
    public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException {
        return checkAndMutate(checkAndMutate, HConstants.NO_NONCE, HConstants.NO_NONCE);
    }

    public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate, long nonceGroup,
            long nonce) throws IOException {
        return TraceUtil.trace(() -> checkAndMutateInternal(checkAndMutate, nonceGroup, nonce),
                () -> createRegionSpan("Region.checkAndMutate"));
    }

    Span createRegionSpan(String name) {
        return TraceUtil.createSpan(name).setAttribute(REGION_NAMES_KEY,
                Collections.singletonList(getRegionInfo().getRegionNameAsString()));
    }

    private CheckAndMutateResult checkAndMutateInternal(CheckAndMutate checkAndMutate,
            long nonceGroup, long nonce) throws IOException {
        byte[] row = checkAndMutate.getRow();
        Filter filter = null;
        byte[] family = null;
        byte[] qualifier = null;
        CompareOperator op = null;
        ByteArrayComparable comparator = null;
        if (checkAndMutate.hasFilter()) {
            filter = checkAndMutate.getFilter();
        } else {
            family = checkAndMutate.getFamily();
            qualifier = checkAndMutate.getQualifier();
            op = checkAndMutate.getCompareOp();
            comparator = new BinaryComparator(checkAndMutate.getValue());
        }
        TimeRange timeRange = checkAndMutate.getTimeRange();

        Mutation mutation = null;
        RowMutations rowMutations = null;
        if (checkAndMutate.getAction() instanceof Mutation) {
            mutation = (Mutation) checkAndMutate.getAction();
        } else {
            rowMutations = (RowMutations) checkAndMutate.getAction();
        }

        if (mutation != null) {
            checkMutationType(mutation);
            checkRow(mutation, row);
        } else {
            checkRow(rowMutations, row);
        }
        checkReadOnly();
        // TODO, add check for value length also move this check to the client
        checkResources();
        startRegionOperation();
        try {
            Get get = new Get(row);
            if (family != null) {
                checkFamily(family);
                get.addColumn(family, qualifier);
            }
            if (filter != null) {
                get.setFilter(filter);
            }
            if (timeRange != null) {
                get.setTimeRange(timeRange.getMin(), timeRange.getMax());
            }
            // Lock row - note that doBatchMutate will relock this row if called
            checkRow(row, "doCheckAndRowMutate");
            RowLock rowLock = getRowLock(get.getRow(), false, null);
            try {

                // NOTE: We used to wait here until mvcc caught up: mvcc.await();
                // Supposition is that now all changes are done under row locks, then when we go
                // to read,
                // we'll get the latest on this row.
                boolean matches = false;
                long cellTs = 0;
                try (RegionScanner scanner = getScanner(new Scan(get))) {
                    // NOTE: Please don't use HRegion.get() instead,
                    // because it will copy cells to heap. See HBASE-26036
                    List<Cell> result = new ArrayList<>(1);
                    scanner.next(result);
                    if (filter != null) {
                        if (!result.isEmpty()) {
                            matches = true;
                            cellTs = result.get(0).getTimestamp();
                        }
                    } else {
                        boolean valueIsNull = comparator.getValue() == null || comparator.getValue().length == 0;
                        if (result.isEmpty() && valueIsNull) {
                            matches = op != CompareOperator.NOT_EQUAL;
                        } else if (result.size() > 0 && valueIsNull) {
                            matches = (result.get(0).getValueLength() == 0) == (op != CompareOperator.NOT_EQUAL);
                            cellTs = result.get(0).getTimestamp();
                        } else if (result.size() == 1) {
                            Cell kv = result.get(0);
                            cellTs = kv.getTimestamp();
                            int compareResult = PrivateCellUtil.compareValue((org.waterme7on.hbase.Cell) kv,
                                    comparator);
                            matches = matches(op, compareResult);
                        }
                    }
                }

                // If matches, perform the mutation or the rowMutations
                if (matches) {
                    // We have acquired the row lock already. If the system clock is NOT
                    // monotonically
                    // non-decreasing (see HBASE-14070) we should make sure that the mutation has a
                    // larger timestamp than what was observed via Get. doBatchMutate already does
                    // this, but
                    // there is no way to pass the cellTs. See HBASE-14054.
                    long now = EnvironmentEdgeManager.currentTime();
                    long ts = Math.max(now, cellTs); // ensure write is not eclipsed
                    byte[] byteTs = Bytes.toBytes(ts);
                    if (mutation != null) {
                        if (mutation instanceof Put) {
                            updateCellTimestamps(mutation.getFamilyCellMap().values(), byteTs);
                        }
                        // And else 'delete' is not needed since it already does a second get, and sets
                        // the
                        // timestamp from get (see prepareDeleteTimestamps).
                    } else {
                        for (Mutation m : rowMutations.getMutations()) {
                            if (m instanceof Put) {
                                updateCellTimestamps(m.getFamilyCellMap().values(), byteTs);
                            }
                        }
                        // And else 'delete' is not needed since it already does a second get, and sets
                        // the
                        // timestamp from get (see prepareDeleteTimestamps).
                    }
                    // All edits for the given row (across all column families) must happen
                    // atomically.
                    Result r;
                    if (mutation != null) {
                        r = mutate(mutation, true, nonceGroup, nonce).getResult();
                    } else {
                        r = mutateRow(rowMutations, nonceGroup, nonce);
                    }
                    return new CheckAndMutateResult(true, r);
                }
                return new CheckAndMutateResult(false, null);
            } finally {
                rowLock.release();
            }
        } finally {
            closeRegionOperation();
        }
    }

    @Override
    public void delete(Delete delete) throws IOException {
        TraceUtil.trace(() -> {
            checkReadOnly();
            checkResources();
            startRegionOperation(Operation.DELETE);
            try {
                // All edits for the given row (across all column families) must happen
                // atomically.
                return mutate(delete);
            } finally {
                closeRegionOperation(Operation.DELETE);
            }
        }, () -> createRegionSpan("Region.delete"));
    }

    @Override
    public Result get(Get get) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'get'");
    }

    @Override
    public RegionScanner getScanner(Scan scan) throws IOException {
        // TODO
        return null;
    }

    @Override
    public CellComparator getCellComparator() {
        return cellComparator;
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        return increment(increment, HConstants.NO_NONCE, HConstants.NO_NONCE);
    }

    public Result increment(Increment increment, long nonceGroup, long nonce) throws IOException {
        return TraceUtil.trace(() -> {
            checkReadOnly();
            checkResources();
            startRegionOperation(Operation.INCREMENT);
            try {
                // All edits for the given row (across all column families) must happen
                // atomically.
                return mutate(increment, true, nonceGroup, nonce).getResult();
            } finally {
                closeRegionOperation(Operation.INCREMENT);
            }
        }, () -> createRegionSpan("Region.increment"));
    }

    @Override
    public Result mutateRow(RowMutations mutations) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'mutateRow'");
    }

    @Override
    public void mutateRowsWithLocks(Collection<Mutation> mutations, Collection<byte[]> rowsToLock, long nonceGroup,
            long nonce) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'mutateRowsWithLocks'");
    }

    @Override
    public void processRowsWithLocks(RowProcessor<?, ?> processor) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processRowsWithLocks'");
    }

    @Override
    public void processRowsWithLocks(RowProcessor<?, ?> processor, long nonceGroup, long nonce) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processRowsWithLocks'");
    }

    @Override
    public void processRowsWithLocks(RowProcessor<?, ?> processor, long timeout, long nonceGroup, long nonce)
            throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processRowsWithLocks'");
    }

    @Override
    public void put(Put put) throws IOException {
        TraceUtil.trace(() -> {
            checkReadOnly();

            // Do a rough check that we have resources to accept a write. The check is
            // 'rough' in that between the resource check and the call to obtain a
            // read lock, resources may run out. For now, the thought is that this
            // will be extremely rare; we'll deal with it when it happens.
            checkResources();
            startRegionOperation(Operation.PUT);
            try {
                // All edits for the given row (across all column families) must happen
                // atomically.
                return mutate(put);
            } finally {
                closeRegionOperation(Operation.PUT);
            }
        }, () -> createRegionSpan("Region.put"));
    }

    @Override
    public CompactionState getCompactionState() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCompactionState'");
    }

    @Override
    public boolean waitForFlushes(long timeout) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'waitForFlushes'");
    }

    @Override
    public Configuration getReadOnlyConfiguration() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReadOnlyConfiguration'");
    }

    /**
     * Determines if the specified row is within the row range specified by the
     * specified RegionInfo
     * 
     * @param info RegionInfo that specifies the row range
     * @param row  row to be checked
     * @return true if the row is within the range specified by the RegionInfo
     */
    public static boolean rowIsInRange(RegionInfo info, final byte[] row) {
        return ((info.getStartKey().length == 0) || (Bytes.compareTo(info.getStartKey(), row) <= 0))
                && ((info.getEndKey().length == 0) || (Bytes.compareTo(info.getEndKey(), row) > 0));
    }

    public static HRegion createHRegion(RegionInfo newRegion, Path rootDir, Configuration conf,
            TableDescriptor tableDescriptor, WAL wal) throws IOException {
        return createHRegion(newRegion, rootDir, conf, tableDescriptor, wal, true);
    }

    public static HRegion createHRegion(RegionInfo newRegion, Path rootDir, Configuration conf,
            TableDescriptor tableDescriptor, WAL wal, boolean initialize) throws IOException {
        return createHRegion(newRegion, rootDir, conf, tableDescriptor, wal, initialize, null);
    }

    /**
     * Convenience method creating new HRegions. Used by createTable.
     * 
     * @param info          Info for region to create.
     * @param rootDir       Root directory for HBase instance
     * @param wal           shared WAL
     * @param initialize    - true to initialize the region
     * @param rsRpcServices An interface we can request flushes against.
     * @return new HRegion
     */
    public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
            final Configuration conf, final TableDescriptor hTableDescriptor, final WAL wal,
            final boolean initialize, RegionServerServices rsRpcServices) throws IOException {
        LOG.info("creating " + info + ", tableDescriptor="
                + (hTableDescriptor == null ? "null" : hTableDescriptor) + ", regionDir=" + rootDir);
        createRegionDir(conf, info, rootDir);
        FileSystem fs = rootDir.getFileSystem(conf);
        Path tableDir = CommonFSUtils.getTableDir(rootDir, info.getTable());
        HRegion region = HRegion.newHRegion(tableDir, wal, fs, conf, info, hTableDescriptor, rsRpcServices);
        if (initialize) {
            region.initialize(null);
        }
        return region;
    }

    /**
     * Create the region directory in the filesystem.
     */
    public static HRegionFileSystem createRegionDir(Configuration configuration, RegionInfo ri,
            Path rootDir) throws IOException {
        FileSystem fs = rootDir.getFileSystem(configuration);
        Path tableDir = CommonFSUtils.getTableDir(rootDir, ri.getTable());
        // If directory already exists, will log warning and keep going. Will try to
        // create
        // .regioninfo. If one exists, will overwrite.
        return HRegionFileSystem.createRegionOnFileSystem(configuration, fs, tableDir, ri);
    }

    private long initializeRegionInternals(
            final MonitoredTask status) throws IOException {
        // Write HRI to a file in case we need to recover hbase:meta
        // Only the primary replica should write .regioninfo
        if (this.getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
            status.setStatus("Writing region info on filesystem");
            fs.checkRegionInfoOnFilesystem();
        }

        // Initialize all the HStores
        status.setStatus("Initializing all the Stores");
        long maxSeqId = initializeStores(status);
        // // this.mvcc.advanceTo(maxSeqId);

        this.lastReplayedOpenRegionSeqId = maxSeqId;

        this.writestate.setReadOnly(false);
        this.writestate.flushRequested = false;
        this.writestate.compacting.set(0);

        if (this.writestate.writesEnabled) {
            // Remove temporary data left over from old regions
            status.setStatus("Cleaning up temporary data from old regions");
            fs.cleanupTempDir();
        }

        long lastFlushTime = EnvironmentEdgeManager.currentTime();
        for (HStore store : stores.values()) {
            this.lastStoreFlushTimeMap.put(store, lastFlushTime);
        }

        // Use maximum of log sequenceid or that which was found in stores
        // (particularly if no recovered edits, seqid will be -1).
        long nextSeqId = maxSeqId + 1;

        // A region can be reopened if failed a split; reset flags
        this.closing.set(false);
        this.closed.set(false);

        status.markComplete("Region opened successfully");
        return nextSeqId;
    }

    /**
     * Open all Stores.
     * 
     * @return Highest sequenceId found out in a Store.
     */
    private long initializeStores(MonitoredTask status)
            throws IOException {
        return initializeStores(status, false);
    }

    private long initializeStores(MonitoredTask status,
            boolean warmup) throws IOException {
        // Load in all the HStores.
        long maxSeqId = -1;
        // initialized to -1 so that we pick up MemstoreTS from column families
        long maxMemstoreTS = -1;

        if (htableDescriptor.getColumnFamilyCount() != 0) {
            // initialize the thread pool for opening stores in parallel.
            ThreadPoolExecutor storeOpenerThreadPool = getStoreOpenAndCloseThreadPool(
                    "StoreOpener-" + this.getRegionInfo().getShortNameToLog());
            CompletionService<HStore> completionService = new ExecutorCompletionService<>(storeOpenerThreadPool);

            // initialize each store in parallel
            for (final ColumnFamilyDescriptor family : htableDescriptor.getColumnFamilies()) {
                status.setStatus("Instantiating store for column family " + family);
                completionService.submit(new Callable<HStore>() {
                    @Override
                    public HStore call() throws IOException {
                        return instantiateHStore(family, warmup);
                    }
                });
            }
            boolean allStoresOpened = false;
            try {
                for (int i = 0; i < htableDescriptor.getColumnFamilyCount(); i++) {
                    Future<HStore> future = completionService.take();
                    HStore store = future.get();
                    this.stores.put(store.getColumnFamilyDescriptor().getName(), store);

                    // long storeMaxSequenceId = store.getMaxSequenceId().orElse(0L);
                    // maxSeqIdInStores.put(Bytes.toBytes(store.getColumnFamilyName()),
                    // storeMaxSequenceId);
                    // if (maxSeqId == -1 || storeMaxSequenceId > maxSeqId) {
                    // maxSeqId = storeMaxSequenceId;
                    // }
                    // long maxStoreMemstoreTS = store.getMaxMemStoreTS().orElse(0L);
                    // if (maxStoreMemstoreTS > maxMemstoreTS) {
                    // maxMemstoreTS = maxStoreMemstoreTS;
                    // }
                }
                allStoresOpened = true;
            } catch (InterruptedException e) {
                throw throwOnInterrupt(e);
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            } finally {
                storeOpenerThreadPool.shutdownNow();
                if (!allStoresOpened) {
                    // something went wrong, close all opened stores
                    LOG.error("Could not initialize all stores for the region=" + this);
                    for (HStore store : this.stores.values()) {
                        try {
                            store.close();
                        } catch (IOException e) {
                            LOG.warn("close store {} failed in region {}", store.toString(), this, e);
                        }
                    }
                }
            }
        }
        return Math.max(maxSeqId, maxMemstoreTS + 1);
    }

    private void dropMemStoreContents() {
    }

    /**
     * Initialize this region.
     * 
     * @param reporter Tickle every so often if initialize is taking a while.
     * @return What the next sequence (edit) id should be.
     */
    long initialize(final Object reporter) throws IOException {

        // Refuse to open the region if there is no column family in the table
        if (htableDescriptor.getColumnFamilyCount() == 0) {
            throw new DoNotRetryIOException("Table " + htableDescriptor.getTableName().getNameAsString()
                    + " should have at least one column family.");
        }

        MonitoredTask status = TaskMonitor.get().createStatus("Initializing region " + this, true);
        long nextSeqId = -1;
        try {
            nextSeqId = initializeRegionInternals(status);
            return nextSeqId;
        } catch (IOException e) {
            LOG.warn("Failed initialize of region= {}, starting to roll back memstore",
                    getRegionInfo().getRegionNameAsString(), e);
            // drop the memory used by memstore if open region fails
            dropMemStoreContents();
            throw e;
        } finally {
            // nextSeqid will be -1 if the initialization fails.
            // At least it will be 0 otherwise.
            if (nextSeqId == -1) {
                status.abort("Exception during region " + getRegionInfo().getRegionNameAsString()
                        + " initialization.");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Region open journal for {}:\n{}", this.getRegionInfo().getEncodedName(),
                        status.prettyPrintJournal());
            }
            status.cleanup();
        }
    }

    public void close() {
    }

    //////////////////////////////////////////////////////////////////////////////
    // Support code
    //////////////////////////////////////////////////////////////////////////////
    protected HStore instantiateHStore(final ColumnFamilyDescriptor family, boolean warmup)
            throws IOException {
        return new HStore(this, family, this.conf, warmup);
    }

    private ThreadPoolExecutor getStoreOpenAndCloseThreadPool(final String threadNamePrefix) {
        int numStores = Math.max(1, this.htableDescriptor.getColumnFamilyCount());
        int maxThreads = Math.min(numStores, conf.getInt(HConstants.HSTORE_OPEN_AND_CLOSE_THREADS_MAX,
                HConstants.DEFAULT_HSTORE_OPEN_AND_CLOSE_THREADS_MAX));
        return getOpenAndCloseThreadPool(maxThreads, threadNamePrefix);
    }

    private static ThreadPoolExecutor getOpenAndCloseThreadPool(int maxThreads,
            final String threadNamePrefix) {
        return Threads.getBoundedCachedThreadPool(maxThreads, 30L, TimeUnit.SECONDS,
                new ThreadFactory() {
                    private int count = 1;

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, threadNamePrefix + "-" + count++);
                    }
                });
    }

    // Utility methods
    /**
     * A utility method to create new instances of HRegion based on the
     * {@link HConstants#REGION_IMPL}
     * configuration property.
     * 
     * @param tableDir   qualified path of directory where region should be located,
     *                   usually the table
     *                   directory.
     * @param wal        The WAL is the outbound log for any updates to the HRegion
     *                   The wal file is a
     *                   logfile from the previous execution that's custom-computed
     *                   for this HRegion.
     *                   The HRegionServer computes and sorts the appropriate wal
     *                   info for this
     *                   HRegion. If there is a previous file (implying that the
     *                   HRegion has been
     *                   written-to before), then read it from the supplied path.
     * @param fs         is the filesystem.
     * @param conf       is global configuration settings.
     * @param regionInfo - RegionInfo that describes the region is new), then read
     *                   them from the
     *                   supplied path.
     * @param htd        the table descriptor
     * @return the new instance
     */
    public static HRegion newHRegion(Path tableDir, WAL wal, FileSystem fs, Configuration conf,
            RegionInfo regionInfo, final TableDescriptor htd, RegionServerServices rsServices) {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends HRegion> regionClass = (Class<? extends HRegion>) conf.getClass(HConstants.REGION_IMPL,
                    HRegion.class);

            Constructor<? extends HRegion> c = regionClass.getConstructor(Path.class, WAL.class, FileSystem.class,
                    Configuration.class,
                    RegionInfo.class, TableDescriptor.class, RegionServerServices.class);

            return c.newInstance(tableDir, wal, fs, conf, regionInfo, htd, rsServices);
        } catch (Throwable e) {
            // todo: what should I throw here?
            throw new IllegalStateException("Could not instantiate a region instance.", e);
        }
    }

    /** Make sure this is a valid row for the HRegion */
    void checkRow(byte[] row, String op) throws IOException {
        if (!rowIsInRange(getRegionInfo(), row)) {
            throw new WrongRegionException("Requested row out of range for " + op + " on HRegion " + this
                    + ", startKey='" + Bytes.toStringBinary(getRegionInfo().getStartKey()) + "', getEndKey()='"
                    + Bytes.toStringBinary(getRegionInfo().getEndKey()) + "', row='" + Bytes.toStringBinary(row)
                    + "'");
        }
    }

    private void checkMutationType(final Mutation mutation) throws DoNotRetryIOException {
        if (!(mutation instanceof Put) && !(mutation instanceof Delete)
                && !(mutation instanceof Increment) && !(mutation instanceof Append)) {
            throw new org.apache.hadoop.hbase.DoNotRetryIOException(
                    "Action must be Put or Delete or Increment or Delete");
        }
    }

    private void checkRow(final Row action, final byte[] row) throws DoNotRetryIOException {
        if (!Bytes.equals(row, action.getRow())) {
            throw new org.apache.hadoop.hbase.DoNotRetryIOException("Action's getRow must match");
        }
    }

    /**
     * @throws IOException Throws exception if region is in read-only mode.
     */
    private void checkReadOnly() throws IOException {
        if (isReadOnly()) {
            throw new DoNotRetryIOException("region is read only");
        }
    }

    /**
     * Check if resources to support an update.
     * <p/>
     * We throw RegionTooBusyException if above memstore limit and expect client to
     * retry using some
     * kind of backoff
     */
    private void checkResources() throws RegionTooBusyException {
        // If catalog region, do not impose resource constraints or block updates.
        if (this.getRegionInfo().isMetaRegion()) {
            return;
        }

        MemStoreSize mss = this.memStoreSizing.getMemStoreSize();
        if (mss.getHeapSize() + mss.getOffHeapSize() > this.blockingMemStoreSize) {
            blockedRequestsCount.increment();
            requestFlush();
            // Don't print current limit because it will vary too much. The message is used
            // as a key
            // over in RetriesExhaustedWithDetailsException processing.
            final String regionName = this.getRegionInfo() == null ? "unknown" : this.getRegionInfo().getEncodedName();
            final String serverName = this.getRegionServerServices() == null
                    ? "unknown"
                    : (this.getRegionServerServices().getServerName() == null
                            ? "unknown"
                            : this.getRegionServerServices().getServerName().toString());
            RegionTooBusyException rtbe = new RegionTooBusyException("Over memstore limit="
                    + org.apache.hadoop.hbase.procedure2.util.StringUtils.humanSize(this.blockingMemStoreSize)
                    + ", regionName=" + regionName + ", server=" + serverName);
            LOG.warn("Region is too busy due to exceeding memstore size limit.", rtbe);
            throw rtbe;
        }

        LOG.debug("checkResources: ok");
    }

    private boolean matches(final CompareOperator op, final int compareResult) {
        boolean matches = false;
        switch (op) {
            case LESS:
                matches = compareResult < 0;
                break;
            case LESS_OR_EQUAL:
                matches = compareResult <= 0;
                break;
            case EQUAL:
                matches = compareResult == 0;
                break;
            case NOT_EQUAL:
                matches = compareResult != 0;
                break;
            case GREATER_OR_EQUAL:
                matches = compareResult >= 0;
                break;
            case GREATER:
                matches = compareResult > 0;
                break;
            default:
                throw new RuntimeException("Unknown Compare op " + op.name());
        }
        return matches;
    }

    /**
     * Throw the correct exception upon interrupt
     * 
     * @param t cause
     */
    // Package scope for tests
    IOException throwOnInterrupt(Throwable t) {
        if (this.closing.get()) {
            return (NotServingRegionException) new NotServingRegionException(
                    getRegionInfo().getRegionNameAsString() + " is closing").initCause(t);
        }
        return (InterruptedIOException) new InterruptedIOException().initCause(t);
    }

    /**
     * Replace any cell timestamps set to
     * {@link org.apache.hadoop.hbase.HConstants#LATEST_TIMESTAMP}
     * provided current timestamp.
     */
    private static void updateCellTimestamps(final Iterable<List<Cell>> cellItr, final byte[] now)
            throws IOException {
        for (List<Cell> cells : cellItr) {
            if (cells == null)
                continue;
            // Optimization: 'foreach' loop is not used. See:
            // HBASE-12023 HRegion.applyFamilyMapToMemstore creates too many iterator
            // objects
            assert cells instanceof RandomAccess;
            int listSize = cells.size();
            for (int i = 0; i < listSize; i++) {
                PrivateCellUtil.updateLatestStamp(cells.get(i), now);
            }
        }
    }

    //
    // New HBASE-880 Helpers
    //
    void checkFamily(final byte[] family) throws NoSuchColumnFamilyException {
        if (!this.htableDescriptor.hasColumnFamily(family)) {
            throw new NoSuchColumnFamilyException("Column family " + Bytes.toString(family)
                    + " does not exist in region " + this + " in table " + this.htableDescriptor);
        }
    }

    private OperationStatus mutate(Mutation mutation) throws IOException {
        return mutate(mutation, false);
    }

    private OperationStatus mutate(Mutation mutation, boolean atomic) throws IOException {
        return mutate(mutation, atomic, HConstants.NO_NONCE, HConstants.NO_NONCE);
    }

    private OperationStatus mutate(Mutation mutation, boolean atomic, long nonceGroup, long nonce)
            throws IOException {
        OperationStatus[] status = this.batchMutate(new Mutation[] { mutation }, atomic, nonceGroup, nonce);
        if (status[0].getOperationStatusCode().equals(OperationStatusCode.SANITY_CHECK_FAILURE)) {
            throw new FailedSanityCheckException(status[0].getExceptionMsg());
        } else if (status[0].getOperationStatusCode().equals(OperationStatusCode.BAD_FAMILY)) {
            throw new NoSuchColumnFamilyException(status[0].getExceptionMsg());
        } else if (status[0].getOperationStatusCode().equals(OperationStatusCode.STORE_TOO_BUSY)) {
            throw new RegionTooBusyException(status[0].getExceptionMsg());
        }
        return status[0];
    }

    public OperationStatus[] batchMutate(Mutation[] mutations, boolean atomic, long nonceGroup,
            long nonce) throws IOException {
        // As it stands, this is used for 3 things
        // * batchMutate with single mutation - put/delete/increment/append, separate or
        // from
        // checkAndMutate.
        // * coprocessor calls (see ex. BulkDeleteEndpoint).
        // So nonces are not really ever used by HBase. They could be by coprocs, and
        // checkAnd...

        LOG.debug("batchMutate");
        return batchMutate(new MutationBatchOperation(this, mutations, atomic, nonceGroup, nonce));
    }

    private OperationStatus[] batchMutate(BatchOperation<?> batchOp) throws IOException {
        boolean initialized = false;
        try {
            while (!batchOp.isDone()) {
                checkResources();

                if (!initialized) {
                    batchOp.checkAndPrepare();
                    initialized = true;
                }
                doMiniBatchMutate(batchOp);
                requestFlushIfNeeded();
            }
        } finally {
        }
        return batchOp.retCodeDetails;

    }

    /**
     * Called to do a piece of the batch that came in to
     * {@link #batchMutate(Mutation[])} In here we
     * also handle replay of edits on region recover. Also gets change in size
     * brought about by
     * applying {@code batchOp}.
     */
    private void doMiniBatchMutate(BatchOperation<?> batchOp) throws IOException {
        /////////////////////
        LOG.debug("doMiniBatchMutation");
        boolean success = false;
        WALEdit walEdit = null;
        MultiVersionConcurrencyControl.WriteEntry writeEntry = null;
        boolean locked = false;
        List<Mutation> mutations = new ArrayList<>();

        // We try to set up a batch in the range
        // [batchOp.nextIndexToProcess,lastIndexExclusive)
        MiniBatchOperationInProgress<Mutation> miniBatchOp = null;
        /** Keep track of the locks we hold so we can release them in finally clause */
        List<Region.RowLock> acquiredRowLocks = Lists.newArrayListWithCapacity(batchOp.size());

        // Check for thread interrupt status in case we have been signaled from
        // #interruptRegionOperation.
        checkInterrupt();

        try {
            // STEP 1. Try to acquire as many locks as we can and build mini-batch of
            // operations with
            // locked rows
            miniBatchOp = batchOp.lockRowsAndBuildMiniBatch(acquiredRowLocks);

            // We've now grabbed as many mutations off the list as we can
            // Ensure we acquire at least one.
            if (miniBatchOp.getReadyToWriteCount() <= 0) {
                // Nothing to put/delete/increment/append -- an exception in the above such as
                // NoSuchColumnFamily?
                return;
            }
            // Check for thread interrupt status in case we have been signaled from
            // #interruptRegionOperation. Do it before we take the lock and disable
            // interrupts for
            // the WAL append.
            checkInterrupt();

            lock(this.updatesLock.readLock(), miniBatchOp.getReadyToWriteCount());
            locked = true;

            // From this point until memstore update this operation should not be
            // interrupted.
            disableInterrupts();

            // STEP 2. Update mini batch of all operations in progress with LATEST_TIMESTAMP
            // timestamp
            // We should record the timestamp only after we have acquired the rowLock,
            // otherwise, newer puts/deletes/increment/append are not guaranteed to have a
            // newer
            // timestamp

            long now = EnvironmentEdgeManager.currentTime();
            batchOp.prepareMiniBatchOperations(miniBatchOp, now, acquiredRowLocks);

            // STEP 3. Build WAL edit
            List<Pair<NonceKey, WALEdit>> walEdits = batchOp.buildWALEdits(miniBatchOp);

            // STEP 4. Append the WALEdits to WAL and sync.
            // for (Iterator<Pair<NonceKey, org.apache.hadoop.hbase.wal.WALEdit>> it =
            // walEdits.iterator(); it.hasNext();) {
            // Pair<NonceKey, org.apache.hadoop.hbase.wal.WALEdit> nonceKeyWALEditPair =
            // it.next();
            // walEdit = nonceKeyWALEditPair.getSecond();
            // NonceKey nonceKey = nonceKeyWALEditPair.getFirst();
            //
            // if (walEdit != null && !walEdit.isEmpty()) {
            // writeEntry = doWALAppend(walEdit, batchOp.durability,
            // batchOp.getClusterIds(), now,
            // nonceKey.getNonceGroup(), nonceKey.getNonce(), batchOp.getOrigLogSeqNum());
            // }
            //
            // // Complete mvcc for all but last writeEntry (for replay case)
            // if (it.hasNext() && writeEntry != null) {
            // mvcc.complete(writeEntry);
            // writeEntry = null;
            // }
            // }

            // STEP 5. Write back to memStore
            // NOTE: writeEntry can be null here
            writeEntry = batchOp.writeMiniBatchOperationsToMemStore(miniBatchOp, writeEntry);

            // STEP 6. Complete MiniBatchOperations: If required calls postBatchMutate() CP
            // hook and
            // complete mvcc for last writeEntry
            batchOp.completeMiniBatchOperations(miniBatchOp, writeEntry);
            writeEntry = null;
            success = true;
        } finally {

            // Call complete rather than completeAndWait because we probably had error if
            // walKey != null
            // if (writeEntry != null) mvcc.complete(writeEntry);

            if (locked) {
                this.updatesLock.readLock().unlock();
            }
            releaseRowLocks(acquiredRowLocks);

            enableInterrupts();

            final int finalLastIndexExclusive = miniBatchOp != null ? miniBatchOp.getLastIndexExclusive()
                    : batchOp.size();
            final boolean finalSuccess = success;
            batchOp.visitBatchOperations(true, finalLastIndexExclusive, (int i) -> {
                Mutation mutation = batchOp.getMutation(i);
                if (mutation instanceof Increment || mutation instanceof Append) {
                    if (finalSuccess) {
                        batchOp.retCodeDetails[i] = new OperationStatus(OperationStatusCode.SUCCESS,
                                batchOp.results[i]);
                    } else {
                        batchOp.retCodeDetails[i] = OperationStatus.FAILURE;
                    }
                } else {
                    batchOp.retCodeDetails[i] = finalSuccess ? OperationStatus.SUCCESS : OperationStatus.FAILURE;
                }
                return true;
            });

            batchOp.doPostOpCleanupForMiniBatch(miniBatchOp, walEdit, finalSuccess);

            batchOp.nextIndexToProcess = finalLastIndexExclusive;

        }
    }

    private void requestFlushIfNeeded() {
        // TODO
    }

    /**
     * If a handler thread is eligible for interrupt, make it ineligible. Should be
     * paired with
     * {{@link #enableInterrupts()}.
     */
    void disableInterrupts() {
        regionLockHolders.computeIfPresent(Thread.currentThread(), (t, b) -> false);
    }

    /**
     * If a handler thread was made ineligible for interrupt via
     * {{@link #disableInterrupts()}, make
     * it eligible again. No-op if interrupts are already enabled.
     */
    void enableInterrupts() {
        regionLockHolders.computeIfPresent(Thread.currentThread(), (t, b) -> true);
    }

    public Result mutateRow(RowMutations rm, long nonceGroup, long nonce) throws IOException {
        final List<Mutation> m = rm.getMutations();
        OperationStatus[] statuses = batchMutate(m.toArray(new Mutation[0]), true, nonceGroup, nonce);

        List<Result> results = new ArrayList<>();
        for (OperationStatus status : statuses) {
            if (status.getResult() != null) {
                results.add(status.getResult());
            }
        }

        if (results.isEmpty()) {
            return null;
        }

        // Merge the results of the Increment/Append operations
        List<Cell> cells = new ArrayList<>();
        for (Result result : results) {
            if (result.rawCells() != null) {
                cells.addAll(Arrays.asList(result.rawCells()));
            }
        }
        return Result.create(cells);
    }

    private void requestFlush() {
        // TODO
    }

    private void releaseRowLocks(List<RowLock> rowLocks) {
        if (rowLocks != null) {
            for (RowLock rowLock : rowLocks) {
                rowLock.release();
            }
            rowLocks.clear();
        }
    }

    /**
     * 
     * Static classes
     * 
     */
    /**
     * Class that tracks the progress of a batch operations, accumulating status
     * codes and tracking
     * the index at which processing is proceeding. These batch operations may get
     * split into
     * mini-batches for processing.
     */
    private abstract static class BatchOperation<T> {
        public final T[] operations;
        protected final OperationStatus[] retCodeDetails;
        // For Increment/Append operations
        protected final Result[] results;
        public final HRegion region;
        // reference family cell maps directly so coprocessors can mutate them if
        // desired
        protected final Map<byte[], List<Cell>>[] familyCellMaps;

        protected final WALEdit[] walEditsFromCoprocessors;
        // Durability of the batch (highest durability of all operations)
        public Durability durability;
        public boolean atomic = false;
        private int nextIndexToProcess;

        abstract public void checkAndPrepare() throws IOException;

        public BatchOperation(final HRegion region, T[] operations) {
            this.operations = operations;
            this.retCodeDetails = new OperationStatus[operations.length];
            this.walEditsFromCoprocessors = new WALEdit[operations.length];
            familyCellMaps = new Map[operations.length];
            Arrays.fill(this.retCodeDetails, OperationStatus.NOT_RUN);
            this.region = region;
            durability = Durability.USE_DEFAULT;
            this.nextIndexToProcess = 0;
            this.results = new Result[operations.length];
        }

        public boolean isDone() {
            return nextIndexToProcess == operations.length;
        }

        /**
         * This method is potentially expensive and useful mostly for non-replay CP
         * path.
         */
        public abstract Mutation[] getMutationsForCoprocs();

        boolean isAtomic() {
            return atomic;
        }

        public int size() {
            return operations.length;
        }

        public abstract Mutation getMutation(int index);

        /**
         * Write mini-batch operations to MemStore
         */
        public abstract MultiVersionConcurrencyControl.WriteEntry writeMiniBatchOperationsToMemStore(
                final MiniBatchOperationInProgress<Mutation> miniBatchOp,
                final MultiVersionConcurrencyControl.WriteEntry writeEntry)
                throws IOException;

        public boolean isOperationPending(int index) {
            return retCodeDetails[index].getOperationStatusCode() == OperationStatusCode.NOT_RUN;
        }

        /**
         * This method completes mini-batch operations by calling postBatchMutate() CP
         * hook (if
         * required) and completing mvcc.
         */
        public void completeMiniBatchOperations(
                final MiniBatchOperationInProgress<Mutation> miniBatchOp,
                final MultiVersionConcurrencyControl.WriteEntry writeEntry)
                throws IOException {
            if (writeEntry != null) {
                // region.mvcc.completeAndWait(writeEntry);
            }
        }

        protected void writeMiniBatchOperationsToMemStore(
                final MiniBatchOperationInProgress<Mutation> miniBatchOp, final long writeNumber)
                throws IOException {
            // TODO
            // org.apache.hadoop.hbase.regionserver.MemStoreSizing memStoreAccounting = new
            // NonThreadSafeMemStoreSizing();
            // visitBatchOperations(true, miniBatchOp.getLastIndexExclusive(), (int index)
            // -> {
            // // We need to update the sequence id for following reasons.
            // // 1) If the op is in replay mode, FSWALEntry#stampRegionSequenceId won't
            // stamp sequence id.
            // // 2) If no WAL, FSWALEntry won't be used
            // // we use durability of the original mutation for the mutation passed by CP.
            // if (isInReplay() || getMutation(index).getDurability() ==
            // Durability.SKIP_WAL) {
            // region.updateSequenceId(familyCellMaps[index].values(), writeNumber);
            // }
            // applyFamilyMapToMemStore(familyCellMaps[index], memStoreAccounting);
            // return true;
            // });
            // // update memStore size
            // region.incMemStoreSize(memStoreAccounting.getDataSize(),
            // memStoreAccounting.getHeapSize(),
            // memStoreAccounting.getOffHeapSize(), memStoreAccounting.getCellsCount());
        }

        /**
         * Helper method that checks and prepares only one mutation. This can be used to
         * implement
         * {@link #checkAndPrepare()} for entire Batch. NOTE: As CP
         * prePut()/preDelete()/preIncrement()/preAppend() hooks may modify mutations,
         * this method
         * should be called after prePut()/preDelete()/preIncrement()/preAppend() CP
         * hooks are run for
         * the mutation
         */
        protected void checkAndPrepareMutation(Mutation mutation, final long timestamp)
                throws IOException {
            region.checkRow(mutation.getRow(), "batchMutate");
            // if (mutation instanceof Put) {
            // // Check the families in the put. If bad, skip this one.
            // checkAndPreparePut((Put) mutation);
            // region.checkTimestamps(mutation.getFamilyCellMap(), timestamp);
            // } else if (mutation instanceof Delete) {
            // region.prepareDelete((Delete) mutation);
            // } else if (mutation instanceof Increment || mutation instanceof Append) {
            // region.checkFamilies(mutation.getFamilyCellMap().keySet());
            // }
        }

        protected void checkAndPrepareMutation(int index, long timestamp) throws IOException {
            Mutation mutation = getMutation(index);
            try {
                this.checkAndPrepareMutation(mutation, timestamp);
                if (mutation instanceof Put || mutation instanceof Delete) {
                // store the family map reference to allow for mutations
                familyCellMaps[index] = mutation.getFamilyCellMap();
                }
            } finally {

            }
        }

        public MiniBatchOperationInProgress<Mutation> lockRowsAndBuildMiniBatch(List<Region.RowLock> acquiredRowLocks)
                throws IOException {
            int readyToWriteCount = 0;
            int lastIndexExclusive = 0;
            Region.RowLock prevRowLock = null;
            for (; lastIndexExclusive < size(); lastIndexExclusive++) {
                // It reaches the miniBatchSize, stop here and process the miniBatch
                // This only applies to non-atomic batch operations.
                if (!isAtomic() && (readyToWriteCount == region.miniBatchSize)) {
                    break;
                }

                if (!isOperationPending(lastIndexExclusive)) {
                    continue;
                }

                Mutation mutation = getMutation(lastIndexExclusive);
                // If we haven't got any rows in our batch, we should block to get the next one.
                Region.RowLock rowLock = null;
                boolean throwException = false;
                try {
                    // if atomic then get exclusive lock, else shared lock
                    rowLock = region.getRowLock(mutation.getRow(), !isAtomic(), prevRowLock);
                    LOG.debug("Successfully got lock, row={}, in region {}", Bytes.toStringBinary(mutation.getRow()),
                            this);
                } catch (TimeoutIOException | InterruptedIOException e) {
                    // NOTE: We will retry when other exceptions, but we should stop if we receive
                    // TimeoutIOException or InterruptedIOException as operation has timed out or
                    // interrupted respectively.
                    LOG.debug("Timeout getting lock, row={}, in region {}", Bytes.toStringBinary(mutation.getRow()),
                            this);
                    throwException = true;
                    throw e;
                } catch (IOException ioe) {
                    LOG.warn("Failed getting lock, row={}, in region {}",
                            Bytes.toStringBinary(mutation.getRow()), this, ioe);
                    if (isAtomic()) { // fail, atomic means all or none
                        throwException = true;
                        throw ioe;
                    }
                } catch (Throwable throwable) {
                    throwException = true;
                    throw throwable;
                } finally {
                    // if (throwException) {
                    // region.storeHotnessProtector.finish(curFamilyCellMap);
                    // }
                }
                if (rowLock == null) {
                    // We failed to grab another lock
                    if (isAtomic()) {
                        // region.storeHotnessProtector.finish(curFamilyCellMap);
                        throw new IOException("Can't apply all operations atomically!");
                    }
                    LOG.debug("We failed to grab another lock");
                    break; // Stop acquiring more rows for this batch
                } else {
                    if (rowLock != prevRowLock) {
                        // It is a different row now, add this to the acquiredRowLocks and
                        // set prevRowLock to the new returned rowLock
                        acquiredRowLocks.add(rowLock);
                        prevRowLock = rowLock;
                    }
                }

                readyToWriteCount++;
            }
            return createMiniBatch(lastIndexExclusive, readyToWriteCount);
        }

        protected MiniBatchOperationInProgress<Mutation> createMiniBatch(final int lastIndexExclusive,
                final int readyToWriteCount) {
            return new MiniBatchOperationInProgress<>(getMutationsForCoprocs(), retCodeDetails,
                    walEditsFromCoprocessors, nextIndexToProcess, lastIndexExclusive, readyToWriteCount);
        }

        /**
         * If necessary, calls preBatchMutate() CP hook for a mini-batch and updates
         * metrics, cell
         * count, tags and timestamp for all cells of all operations in a mini-batch.
         */
        public abstract void prepareMiniBatchOperations(
                MiniBatchOperationInProgress<Mutation> miniBatchOp, long timestamp,
                final List<Region.RowLock> acquiredRowLocks) throws IOException;

        /**
         * Visitor interface for batch operations
         */
        @FunctionalInterface
        interface Visitor {
            /**
             * @param index operation index
             * @return If true continue visiting remaining entries, break otherwise
             */
            boolean visit(int index) throws IOException;
        }

        /**
         * Helper method for visiting pending/ all batch operations
         */
        public void visitBatchOperations(boolean pendingOnly, int lastIndexExclusive, Visitor visitor)
                throws IOException {
            assert lastIndexExclusive <= this.size();
            for (int i = nextIndexToProcess; i < lastIndexExclusive; i++) {
                if (!pendingOnly || isOperationPending(i)) {
                    if (!visitor.visit(i)) {
                        break;
                    }
                }
            }
        }

        public abstract long getNonceGroup(int index);

        public abstract long getNonce(int index);

        public abstract boolean isInReplay();

        public void doPostOpCleanupForMiniBatch(
                final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WALEdit walEdit,
                boolean success) throws IOException {
            // TODO
        }

        /**
         * Builds separate WALEdit per nonce by applying input mutations. If WALEdits
         * from CP are
         * present, they are merged to result WALEdit.
         */
        public List<Pair<NonceKey, WALEdit>> buildWALEdits(final MiniBatchOperationInProgress<Mutation> miniBatchOp)
                throws IOException {
            List<Pair<NonceKey, WALEdit>> walEdits = new ArrayList<>();

            visitBatchOperations(true, nextIndexToProcess + miniBatchOp.size(), new BatchOperation.Visitor() {
                private Pair<NonceKey, WALEdit> curWALEditForNonce;

                @Override
                public boolean visit(int index) throws IOException {
                    // Mutation m = getMutation(index);
                    // // we use durability of the original mutation for the mutation passed by CP.
                    // if (region.getEffectiveDurability(m.getDurability()) == Durability.SKIP_WAL)
                    // {
                    // region.recordMutationWithoutWal(m.getFamilyCellMap());
                    // return true;
                    // }

                    // the batch may contain multiple nonce keys (replay case). If so, write WALEdit
                    // for each.
                    // Given how nonce keys are originally written, these should be contiguous.
                    // They don't have to be, it will still work, just write more WALEdits than
                    // needed.
                    long nonceGroup = getNonceGroup(index);
                    long nonce = getNonce(index);
                    if (curWALEditForNonce == null
                            || curWALEditForNonce.getFirst().getNonceGroup() != nonceGroup
                            || curWALEditForNonce.getFirst().getNonce() != nonce) {
                        curWALEditForNonce = new Pair<>(new NonceKey(nonceGroup, nonce),
                                new WALEdit(miniBatchOp.getCellCount(), isInReplay()));
                        walEdits.add(curWALEditForNonce);
                    }
                    WALEdit walEdit = curWALEditForNonce.getSecond();

                    // Add WAL edits from CPs.
                    WALEdit fromCP = walEditsFromCoprocessors[index];
                    if (fromCP != null) {
                        for (Cell cell : fromCP.getCells()) {
                            walEdit.add(cell);
                        }
                    }
                    walEdit.add(familyCellMaps[index]);

                    return true;
                }
            });
            return walEdits;
        }

    }

    /**
     * Batch of mutation operations. Base class is shared with
     * {@link ReplayBatchOperation} as most of
     * the logic is same.
     */
    private static class MutationBatchOperation extends BatchOperation<Mutation> {
        protected boolean canProceed;
        private long nonceGroup;
        private long nonce;

        public MutationBatchOperation(final HRegion region, Mutation[] operations, boolean atomic,
                long nonceGroup, long nonce) {
            super(region, operations);
            this.atomic = atomic;
            this.nonceGroup = nonceGroup;
            this.nonce = nonce;
        }

        @Override
        public void checkAndPrepare() throws IOException {
            // index 0: puts, index 1: deletes, index 2: increments, index 3: append
            final int[] metrics = { 0, 0, 0, 0 };
            visitBatchOperations(true, this.size(), new Visitor() {
                private long now = EnvironmentEdgeManager.currentTime();
                private WALEdit walEdit;

                @Override
                public boolean visit(int index) throws IOException {
                    // Run coprocessor pre hook outside of locks to avoid deadlock
                    if (walEdit == null) {
                        walEdit = new WALEdit();
                    }
                    // callPreMutateCPHook(index, walEdit, metrics);
                    if (!walEdit.isEmpty()) {
                        walEditsFromCoprocessors[index] = walEdit;
                        walEdit = null;
                    }
                    if (isOperationPending(index)) {
                        // updates are done with different timestamps after acquiring locks. This
                        // behavior is
                        // inherited from the code prior to this change. Can this be changed?
                        checkAndPrepareMutation(index, now);
                    }
                    return true;
                }

            });
        }

        @Override
        public Mutation[] getMutationsForCoprocs() {
            return this.operations;
        }

        @Override
        public Mutation getMutation(int index) {
            return this.operations[index];
        }

        @Override
        public MultiVersionConcurrencyControl.WriteEntry writeMiniBatchOperationsToMemStore(
                MiniBatchOperationInProgress<Mutation> miniBatchOp,
                MultiVersionConcurrencyControl.WriteEntry writeEntry) throws IOException {
            return null;
        }

        private static Get toGet(final Mutation mutation) throws IOException {
            assert mutation instanceof Increment || mutation instanceof Append;
            Get get = new Get(mutation.getRow());
            CellScanner cellScanner = mutation.cellScanner();
            while (!cellScanner.advance()) {
                Cell cell = cellScanner.current();
                get.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
            }
            if (mutation instanceof Increment) {
                // Increment
                Increment increment = (Increment) mutation;
                get.setTimeRange(increment.getTimeRange().getMin(), increment.getTimeRange().getMax());
            } else {
                // Append
                Append append = (Append) mutation;
                get.setTimeRange(append.getTimeRange().getMin(), append.getTimeRange().getMax());
            }
            for (Map.Entry<String, byte[]> entry : mutation.getAttributesMap().entrySet()) {
                get.setAttribute(entry.getKey(), entry.getValue());
            }
            return get;
        }

        @Override
        public void prepareMiniBatchOperations(MiniBatchOperationInProgress<Mutation> miniBatchOp, long timestamp,
                List<RowLock> acquiredRowLocks) throws IOException {
            // For nonce operations
        }

        /**
         * Starts the nonce operation for a mutation, if needed.
         * 
         * @return whether to proceed this mutation.
         */
        private boolean startNonceOperation() throws IOException {
            return true;
        }

        @Override
        public long getNonceGroup(int index) {
            return -1;
        }

        @Override
        public long getNonce(int index) {
            return -1;
        }

        @Override
        public boolean isInReplay() {
            return false;
        }
    }

    class RowLockContext {
        private final HashedBytes row;
        final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
        final AtomicBoolean usable = new AtomicBoolean(true);
        final AtomicInteger count = new AtomicInteger(0);
        final Object lock = new Object();
        private String threadName;

        RowLockContext(HashedBytes row) {
            this.row = row;
        }

        RowLockImpl newWriteLock() {
            Lock l = readWriteLock.writeLock();
            return getRowLock(l);
        }

        RowLockImpl newReadLock() {
            Lock l = readWriteLock.readLock();
            return getRowLock(l);
        }

        private RowLockImpl getRowLock(Lock l) {
            count.incrementAndGet();
            synchronized (lock) {
                if (usable.get()) {
                    return new RowLockImpl(this, l);
                } else {
                    return null;
                }
            }
        }

        void cleanUp() {
            long c = count.decrementAndGet();
            if (c <= 0) {
                synchronized (lock) {
                    if (count.get() <= 0 && usable.get()) { // Don't attempt to remove row if already removed
                        usable.set(false);
                        RowLockContext removed = lockedRows.remove(row);
                        assert removed == this : "we should never remove a different context";
                    }
                }
            }
        }

        public void setThreadName(String threadName) {
            this.threadName = threadName;
        }

        @Override
        public String toString() {
            return "RowLockContext{" + "row=" + row + ", readWriteLock=" + readWriteLock + ", count="
                    + count + ", threadName=" + threadName + '}';
        }
    }

    /**
     * Class used to represent a lock on a row.
     */
    public static class RowLockImpl implements RowLock {
        private final RowLockContext context;
        private final Lock lock;

        public RowLockImpl(RowLockContext context, Lock lock) {
            this.context = context;
            this.lock = lock;
        }

        public Lock getLock() {
            return lock;
        }

        public RowLockContext getContext() {
            return context;
        }

        @Override
        public void release() {
            lock.unlock();
            context.cleanUp();
        }

        @Override
        public String toString() {
            return "RowLockImpl{" + "context=" + context + ", lock=" + lock + '}';
        }
    }

    /*
     * Data structure of write state flags used coordinating flushes, compactions
     * and closes.
     */
    static class WriteState {
        // Set while a memstore flush is happening.
        volatile boolean flushing = false;
        // Set when a flush has been requested.
        volatile boolean flushRequested = false;
        // Number of compactions running.
        AtomicInteger compacting = new AtomicInteger(0);
        // Gets set in close. If set, cannot compact or flush again.
        volatile boolean writesEnabled = true;
        // Set if region is read-only
        volatile boolean readOnly = false;
        // whether the reads are enabled. This is different than readOnly, because
        // readOnly is
        // static in the lifetime of the region, while readsEnabled is dynamic
        volatile boolean readsEnabled = true;

        /**
         * Set flags that make this region read-only.
         * 
         * @param onOff flip value for region r/o setting
         */
        synchronized void setReadOnly(final boolean onOff) {
            this.writesEnabled = !onOff;
            this.readOnly = onOff;
        }

        boolean isReadOnly() {
            return this.readOnly;
        }

        boolean isFlushRequested() {
            return this.flushRequested;
        }

        void setReadsEnabled(boolean readsEnabled) {
            this.readsEnabled = readsEnabled;
        }

        static final long HEAP_SIZE = ClassSize.align(ClassSize.OBJECT + 5 * Bytes.SIZEOF_BOOLEAN);
    }

    /**
     * Check thread interrupt status and throw an exception if interrupted.
     * 
     * @throws NotServingRegionException if region is closing
     * @throws InterruptedIOException    if interrupted but region is not closing
     */
    // Package scope for tests
    void checkInterrupt() throws NotServingRegionException, InterruptedIOException {
        if (Thread.interrupted()) {
            if (this.closing.get()) {
                throw new NotServingRegionException(
                        getRegionInfo().getRegionNameAsString() + " is closing");
            }
            throw new InterruptedIOException();
        }
    }

    private void lock(final Lock lock) throws IOException {
        lock(lock, 1);
    }

    private void lock(final Lock lock, final int multiplier) throws IOException {
        try {
            final long waitTime = Math.min(maxBusyWaitDuration,
                    busyWaitDuration * Math.min(multiplier, maxBusyWaitMultiplier));
            if (!lock.tryLock(waitTime, TimeUnit.MILLISECONDS)) {
                // Don't print millis. Message is used as a key over in
                // RetriesExhaustedWithDetailsException processing.
                final String regionName = this.getRegionInfo() == null ? "unknown"
                        : this.getRegionInfo().getRegionNameAsString();
                final String serverName = this.getRegionServerServices() == null
                        ? "unknown"
                        : (this.getRegionServerServices().getServerName() == null
                                ? "unknown"
                                : this.getRegionServerServices().getServerName().toString());
                RegionTooBusyException rtbe = new RegionTooBusyException(
                        "Failed to obtain lock; regionName=" + regionName + ", server=" + serverName);
                LOG.warn("Region is too busy to allow lock acquisition.", rtbe);
                throw rtbe;
            }
        } catch (InterruptedException ie) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Interrupted while waiting for a lock in region {}", this);
            }
            throw throwOnInterrupt(ie);
        }
    }

    @Override
    public List<Cell> get(Get get, boolean withCoprocessor) throws IOException {
        return get(get, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
    }

    private List<Cell> get(Get get, boolean withCoprocessor, long nonceGroup, long nonce)
            throws IOException {
        return TraceUtil.trace(() -> getInternal(get, withCoprocessor, nonceGroup, nonce),
                () -> createRegionSpan("Region.get"));
    }

    private List<Cell> getInternal(Get get, boolean withCoprocessor, long nonceGroup, long nonce)
            throws IOException {
        List<Cell> results = new ArrayList<>();
        long before = EnvironmentEdgeManager.currentTime();

        Scan scan = new Scan(get);
        try (RegionScanner scanner = getScanner(scan)) {
            List<Cell> tmp = new ArrayList<>();
            scanner.next(tmp);
            // Copy EC to heap, then close the scanner.
            // This can be an EXPENSIVE call. It may make an extra copy from offheap to
            // onheap buffers.
            // See more details in HBASE-26036.
            for (Cell cell : tmp) {
                results.add(CellUtil.cloneIfNecessary(cell));
            }
        }

        // post-get CP hook
        return results;
    }

    /**
     * Set up correct timestamps in the KVs in Delete object.
     * <p/>
     * Caller should have the row and region locks.
     */
    private void prepareDeleteTimestamps(Mutation mutation, Map<byte[], List<Cell>> familyMap,
            byte[] byteNow) throws IOException {
        for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {

            byte[] family = e.getKey();
            List<Cell> cells = e.getValue();
            assert cells instanceof RandomAccess;

            Map<byte[], Integer> kvCount = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            int listSize = cells.size();
            for (int i = 0; i < listSize; i++) {
                Cell cell = cells.get(i);
                // Check if time is LATEST, change to time of most recent addition if so
                // This is expensive.
                if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP
                        && org.apache.hadoop.hbase.PrivateCellUtil.isDeleteType(cell)) {
                    byte[] qual = CellUtil.cloneQualifier(cell);

                    Integer count = kvCount.get(qual);
                    if (count == null) {
                        kvCount.put(qual, 1);
                    } else {
                        kvCount.put(qual, count + 1);
                    }
                    count = kvCount.get(qual);

                    Get get = new Get(CellUtil.cloneRow(cell));
                    get.setMaxVersions(count);
                    get.addColumn(family, qual);
                } else {
                    org.apache.hadoop.hbase.PrivateCellUtil.updateLatestStamp(cell, byteNow);
                }
            }
        }
    }

}
