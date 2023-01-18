package org.waterme7on.hbase;

/**
 * Use {@link CellBuilderFactory} to get CellBuilder instance.
 */
public interface CellBuilder {

    CellBuilder setRow(final byte[] row);

    CellBuilder setRow(final byte[] row, final int rOffset, final int rLength);

    CellBuilder setFamily(final byte[] family);

    CellBuilder setFamily(final byte[] family, final int fOffset, final int fLength);

    CellBuilder setQualifier(final byte[] qualifier);

    CellBuilder setQualifier(final byte[] qualifier, final int qOffset, final int qLength);

    CellBuilder setTimestamp(final long timestamp);

    CellBuilder setType(final Cell.Type type);

    CellBuilder setValue(final byte[] value);

    CellBuilder setValue(final byte[] value, final int vOffset, final int vLength);

    Cell build();

    /**
     * Remove all internal elements from builder.
     */
    CellBuilder clear();
}
