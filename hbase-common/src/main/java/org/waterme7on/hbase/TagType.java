package org.waterme7on.hbase;

public final class TagType {
    // Please declare new Tag Types here to avoid step on pre-existing tag types.
    public static final byte ACL_TAG_TYPE = (byte) 1;
    public static final byte VISIBILITY_TAG_TYPE = (byte) 2;
    // public static final byte LOG_REPLAY_TAG_TYPE = (byte) 3; // deprecated
    public static final byte VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE = (byte) 4;

    // mob tags
    public static final byte MOB_REFERENCE_TAG_TYPE = (byte) 5;
    public static final byte MOB_TABLE_NAME_TAG_TYPE = (byte) 6;

    // String based tag type used in replication
    public static final byte STRING_VIS_TAG_TYPE = (byte) 7;
    public static final byte TTL_TAG_TYPE = (byte) 8;
}
