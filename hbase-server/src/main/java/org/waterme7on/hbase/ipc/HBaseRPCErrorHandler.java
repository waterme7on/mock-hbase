package org.waterme7on.hbase.ipc;

public interface HBaseRPCErrorHandler {
    /**
     * Take actions on the event of an OutOfMemoryError.
     * @param e the throwable
     * @return if the server should be shut down
     */
    boolean checkOOME(final Throwable e);
}
