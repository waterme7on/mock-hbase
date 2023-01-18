package org.waterme7on.hbase.coprocessor;

import org.waterme7on.hbase.procedure2.ProcedureException;

/**
 * Thrown when a procedure is aborted
 */
public class ProcedureAbortedException extends ProcedureException {
    /** default constructor */
    public ProcedureAbortedException() {
        super();
    }

    /**
     * Constructor
     * 
     * @param s message
     */
    public ProcedureAbortedException(String s) {
        super(s);
    }
}
