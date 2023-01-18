package org.waterme7on.hbase;

import java.util.List;

/**
 * Allows creating a cell with {@link Tag} An instance of this type can be
 * acquired by using
 * RegionCoprocessorEnvironment#getCellBuilder (for prod code) and
 * {@link RawCellBuilderFactory}
 * (for unit tests).
 */
public interface RawCellBuilder extends CellBuilder {
    @Override
    RawCellBuilder setRow(final byte[] row);

    @Override
    RawCellBuilder setRow(final byte[] row, final int rOffset, final int rLength);

    @Override
    RawCellBuilder setFamily(final byte[] family);

    @Override
    RawCellBuilder setFamily(final byte[] family, final int fOffset, final int fLength);

    @Override
    RawCellBuilder setQualifier(final byte[] qualifier);

    @Override
    RawCellBuilder setQualifier(final byte[] qualifier, final int qOffset, final int qLength);

    @Override
    RawCellBuilder setTimestamp(final long timestamp);

    @Override
    RawCellBuilder setType(final Cell.Type type);

    @Override
    RawCellBuilder setValue(final byte[] value);

    @Override
    RawCellBuilder setValue(final byte[] value, final int vOffset, final int vLength);

    RawCellBuilder setTags(final List<Tag> tags);

    @Override
    RawCell build();

    @Override
    RawCellBuilder clear();
}
