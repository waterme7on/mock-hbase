package org.waterme7on.hbase.wal;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SequenceIdAccounting {

    public Long startCacheFlush(byte[] encodedRegionName, Map<byte[], Long> familyToSeq) {
        return null;
    }

    public Long startCacheFlush(byte[] encodedRegionName, Set<byte[]> families) {
        return null;
    }

    public void startCacheFlush(byte[] encodedRegionName, long maxFlushedSeqId) {
    }

    public void abortCacheFlush(byte[] encodedRegionName) {
    }

    public long getLowestSequenceId(byte[] encodedRegionName) {
        return 0;
    }

    public long getLowestSequenceId(byte[] encodedRegionName, byte[] familyName) {
        return 0;
    }

    public Map<byte[], List<byte[]>> findLower(Map<byte[], Long> encodedName2HighestSequenceId) {
        return null;
    }

    public boolean areAllLower(Map<byte[], Long> sequenceNums) {
        return false;
    }

    public void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceid, boolean onlyIfGreater) {
    }

    public Map<byte[], Long> resetHighest() {
        return null;
    }

    public void onRegionClose(byte[] encodedRegionName) {
    }

    public void update(byte[] encodedRegionName, Set<byte[]> familyNames, long regionSequenceId, boolean inMemStore) {
    }

}
