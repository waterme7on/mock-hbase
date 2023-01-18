package org.waterme7on.hbase;

class KeyValueBuilder extends ExtendedCellBuilderImpl {

    @Override
    protected ExtendedCell innerBuild() {
        KeyValue kv = new KeyValue(row, rOffset, rLength, family, fOffset, fLength, qualifier, qOffset,
                qLength, timestamp, type, value, vOffset, vLength, tags, tagsOffset, tagsLength);
        kv.setSequenceId(seqId);
        return kv;
    }
}
