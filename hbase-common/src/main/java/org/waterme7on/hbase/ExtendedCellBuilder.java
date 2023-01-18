package org.waterme7on.hbase;

import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * For internal purpose. {@link Tag} and memstoreTS/mvcc are internal
 * implementation detail that
 * should not be exposed publicly. Use {@link ExtendedCellBuilderFactory} to get
 * ExtendedCellBuilder
 * instance. TODO: ditto for ByteBufferExtendedCell?
 */
@InterfaceAudience.Private
public interface ExtendedCellBuilder extends RawCellBuilder {
    @Override
    ExtendedCellBuilder setRow(final byte[] row);

    @Override
    ExtendedCellBuilder setRow(final byte[] row, final int rOffset, final int rLength);

    @Override
    ExtendedCellBuilder setFamily(final byte[] family);

    @Override
    ExtendedCellBuilder setFamily(final byte[] family, final int fOffset, final int fLength);

    @Override
    ExtendedCellBuilder setQualifier(final byte[] qualifier);

    @Override
    ExtendedCellBuilder setQualifier(final byte[] qualifier, final int qOffset, final int qLength);

    @Override
    ExtendedCellBuilder setTimestamp(final long timestamp);

    @Override
    ExtendedCellBuilder setType(final Cell.Type type);

    ExtendedCellBuilder setType(final byte type);

    @Override
    ExtendedCellBuilder setValue(final byte[] value);

    @Override
    ExtendedCellBuilder setValue(final byte[] value, final int vOffset, final int vLength);

    @Override
    ExtendedCell build();

    @Override
    ExtendedCellBuilder clear();

    // we have this method for performance reasons so that if one could create a
    // cell directly from
    // the tag byte[] of the cell without having to convert to a list of Tag(s) and
    // again adding it
    // back.
    ExtendedCellBuilder setTags(final byte[] tags);

    // we have this method for performance reasons so that if one could create a
    // cell directly from
    // the tag byte[] of the cell without having to convert to a list of Tag(s) and
    // again adding it
    // back.
    ExtendedCellBuilder setTags(final byte[] tags, int tagsOffset, int tagsLength);

    @Override
    ExtendedCellBuilder setTags(List<Tag> tags);

    /**
     * Internal usage. Be careful before you use this while building a cell
     * 
     * @param seqId set the seqId
     * @return the current ExternalCellBuilder
     */
    ExtendedCellBuilder setSequenceId(final long seqId);
}
