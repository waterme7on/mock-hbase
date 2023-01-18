package org.waterme7on.hbase;

class IndividualBytesFieldCellBuilder extends ExtendedCellBuilderImpl {

    @Override
    public ExtendedCell innerBuild() {
        return new IndividualBytesFieldCell(row, rOffset, rLength, family, fOffset, fLength, qualifier,
                qOffset, qLength, timestamp, type, seqId, value, vOffset, vLength, tags, tagsOffset,
                tagsLength);
    }
}
