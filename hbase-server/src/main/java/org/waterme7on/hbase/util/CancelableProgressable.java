package org.waterme7on.hbase.util;

/**
 * Similar interface as {@link org.apache.hadoop.util.Progressable} but returns
 * a boolean to support
 * canceling the operation.
 * <p/>
 * Used for doing updating of OPENING znode during log replay on region open.
 */
public interface CancelableProgressable {

    /**
     * Report progress. Returns true if operations should continue, false if the
     * operation should be
     * canceled and rolled back.
     * 
     * @return whether to continue (true) or cancel (false) the operation
     */
    boolean Caprogress();

}
