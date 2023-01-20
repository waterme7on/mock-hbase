package org.waterme7on.hbase.regionserver.store;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.waterme7on.hbase.executor.ExecutorService;
import org.waterme7on.hbase.executor.ExecutorService.ExecutorConfig;
import org.waterme7on.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.waterme7on.hbase.regionserver.HRegion;
import org.waterme7on.hbase.regionserver.RegionServerServices;
import org.waterme7on.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Services a Store needs from a Region. RegionServicesForStores class is the
 * interface through
 * which memstore access services at the region level. For example, when using
 * alternative memory
 * formats or due to compaction the memstore needs to take occasional lock and
 * update size counters
 * at the region level.
 */
@InterfaceAudience.Private
public class RegionServicesForStores {

    private final HRegion region;
    private final RegionServerServices rsServices;
    private int inMemoryPoolSize;

    public RegionServicesForStores(HRegion region, RegionServerServices rsServices) {
        this.region = region;
        this.rsServices = rsServices;
        if (this.rsServices != null) {
            this.inMemoryPoolSize = rsServices.getConfiguration().getInt(
                    CompactingMemStore.IN_MEMORY_CONPACTION_POOL_SIZE_KEY,
                    CompactingMemStore.IN_MEMORY_CONPACTION_POOL_SIZE_DEFAULT);
        }
    }

    public void addMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
            int cellsCountDelta) {
        region.incMemStoreSize(dataSizeDelta, heapSizeDelta, offHeapSizeDelta, cellsCountDelta);
    }

    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    public WAL getWAL() {
        return region.getWAL();
    }

    private static ByteBuffAllocator ALLOCATOR_FOR_TEST;

    private static synchronized ByteBuffAllocator getAllocatorForTest() {
        if (ALLOCATOR_FOR_TEST == null) {
            ALLOCATOR_FOR_TEST = ByteBuffAllocator.HEAP;
        }
        return ALLOCATOR_FOR_TEST;
    }

    public ByteBuffAllocator getByteBuffAllocator() {
        if (rsServices != null && rsServices.getRpcServer() != null) {
            return rsServices.getRpcServer().getByteBuffAllocator();
        } else {
            return getAllocatorForTest();
        }
    }

    private static ThreadPoolExecutor INMEMORY_COMPACTION_POOL_FOR_TEST;

    private static synchronized ThreadPoolExecutor getInMemoryCompactionPoolForTest() {
        if (INMEMORY_COMPACTION_POOL_FOR_TEST == null) {
            INMEMORY_COMPACTION_POOL_FOR_TEST = new ThreadPoolExecutor(10, 10, 60, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setDaemon(true)
                            .setNameFormat("InMemoryCompactionsForTest-%d").build());
        }
        return INMEMORY_COMPACTION_POOL_FOR_TEST;
    }

    ThreadPoolExecutor getInMemoryCompactionPool() {
        if (rsServices != null) {
            ExecutorService executorService = rsServices.getExecutorService();
            ExecutorConfig config = executorService.new ExecutorConfig()
                    .setExecutorType(ExecutorType.RS_IN_MEMORY_COMPACTION).setCorePoolSize(inMemoryPoolSize);
            return executorService.getExecutorLazily(config);
        } else {
            // this could only happen in tests
            return getInMemoryCompactionPoolForTest();
        }
    }

    public long getMemStoreFlushSize() {
        return region.getMemStoreFlushSize();
    }

    public int getNumStores() {
        return region.getTableDescriptor().getColumnFamilyCount();
    }

    long getMemStoreSize() {
        return region.getMemStoreDataSize();
    }
}
