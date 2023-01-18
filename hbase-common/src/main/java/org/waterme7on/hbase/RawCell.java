package org.waterme7on.hbase;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * An extended version of Cell that allows CPs manipulate Tags.
 */
// Added by HBASE-19092 to expose Tags to CPs (history server) w/o exposing
// ExtendedCell.
// Why is this in hbase-common and not in hbase-server where it is used?
// RawCell is an odd name for a class that is only for CPs that want to
// manipulate Tags on
// server-side only w/o exposing ExtendedCell -- super rare, super exotic.
public interface RawCell extends Cell {
    static final int MAX_TAGS_LENGTH = (2 * Short.MAX_VALUE) + 1;

    /**
     * Allows cloning the tags in the cell to a new byte[]
     * 
     * @return the byte[] having the tags
     */
    default byte[] cloneTags() {
        return PrivateCellUtil.cloneTags(this);
    }

    /**
     * Creates a list of tags in the current cell
     * 
     * @return a list of tags
     */
    default Iterator<Tag> getTags() {
        return PrivateCellUtil.tagsIterator(this);
    }

    /**
     * Returns the specific tag of the given type
     * 
     * @param type the type of the tag
     * @return the specific tag if available or null
     */
    default Optional<Tag> getTag(byte type) {
        return PrivateCellUtil.getTag(this, type);
    }

    /**
     * Check the length of tags. If it is invalid, throw IllegalArgumentException
     * 
     * @param tagsLength the given length of tags
     * @throws IllegalArgumentException if tagslength is invalid
     */
    public static void checkForTagsLength(int tagsLength) {
        if (tagsLength > MAX_TAGS_LENGTH) {
            throw new IllegalArgumentException("tagslength " + tagsLength + " > " + MAX_TAGS_LENGTH);
        }
    }

    /** Returns A new cell which is having the extra tags also added to it. */
    public static Cell createCell(Cell cell, List<Tag> tags) {
        return PrivateCellUtil.createCell(cell, tags);
    }
}
