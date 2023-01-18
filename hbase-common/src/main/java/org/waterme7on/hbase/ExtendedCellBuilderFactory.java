package org.waterme7on.hbase;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class ExtendedCellBuilderFactory {

    /**
     * Allows creating a cell with the given CellBuilderType.
     * 
     * @param type the type of CellBuilder(DEEP_COPY or SHALLOW_COPY).
     * @return the cell that is created
     */
    public static ExtendedCellBuilder create(CellBuilderType type) {
        switch (type) {
            case SHALLOW_COPY:
                return new IndividualBytesFieldCellBuilder();
            case DEEP_COPY:
                return new KeyValueBuilder();
            default:
                throw new UnsupportedOperationException("The type:" + type + " is unsupported");
        }
    }

    private ExtendedCellBuilderFactory() {
    }
}
