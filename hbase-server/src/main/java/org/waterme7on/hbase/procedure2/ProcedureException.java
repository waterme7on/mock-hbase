package org.waterme7on.hbase.procedure2;

import org.apache.hadoop.hbase.exceptions.HBaseException;

public class ProcedureException extends HBaseException {
    /** default constructor */
    public ProcedureException() {
        super();
    }

    /**
     * Constructor
     * 
     * @param s message
     */
    public ProcedureException(String s) {
        super(s);
    }

    public ProcedureException(Throwable t) {
        super(t);
    }
}
