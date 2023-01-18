package org.waterme7on.hbase.procedure2;

import java.util.ArrayDeque;

public class ProcedureDeque extends ArrayDeque<Procedure> {
    public ProcedureDeque() {
        // Default is 16 for a list that is rarely used; elements will resize if too
        // small.
        super(2);
    }
}