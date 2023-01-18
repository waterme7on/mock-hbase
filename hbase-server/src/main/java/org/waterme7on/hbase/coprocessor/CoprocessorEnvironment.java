package org.waterme7on.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;

/**
 * Coprocessor environment state.
 */
public interface CoprocessorEnvironment<C extends Coprocessor> {

    /** Returns the Coprocessor interface version */
    int getVersion();

    /** Returns the HBase version as a string (e.g. "0.21.0") */
    String getHBaseVersion();

    /** Returns the loaded coprocessor instance */
    C getInstance();

    /** Returns the priority assigned to the loaded coprocessor */
    int getPriority();

    /** Returns the load sequence number */
    int getLoadSequence();

    /**
     * Returns a Read-only Configuration; throws
     * {@link UnsupportedOperationException} if you try to
     * set a configuration.
     */
    Configuration getConfiguration();

    /** Returns the classloader for the loaded coprocessor instance */
    ClassLoader getClassLoader();
}
