package org.waterme7on.hbase.procedure2;

import java.io.IOException;

/**
 * Special procedure used as a chore. Instead of bringing the Chore class in
 * (dependencies reason),
 * we reuse the executor timeout thread for this special case. The assumption is
 * that procedure is
 * used as hook to dispatch other procedures or trigger some cleanups. It does
 * not store state in
 * the ProcedureStore. this is just for in-memory chore executions.
 */
public abstract class ProcedureInMemoryChore<TEnvironment> extends Procedure<TEnvironment> {
    protected ProcedureInMemoryChore(final int timeoutMsec) {
        setTimeout(timeoutMsec);
    }

    protected abstract void periodicExecute(final TEnvironment env);

    @Override
    protected Procedure<TEnvironment>[] execute(final TEnvironment env) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void rollback(final TEnvironment env) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(final TEnvironment env) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
}
