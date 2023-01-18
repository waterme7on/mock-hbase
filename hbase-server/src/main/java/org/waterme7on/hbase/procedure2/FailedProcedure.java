package org.waterme7on.hbase.procedure2;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

@InterfaceAudience.Private
public class FailedProcedure<TEnvironment> extends Procedure<TEnvironment> {

    private String procName;

    public FailedProcedure() {
    }

    public FailedProcedure(long procId, String procName, User owner, NonceKey nonceKey,
            IOException exception) {
        this.procName = procName;
        setProcId(procId);
        setState(ProcedureState.ROLLEDBACK);
        setOwner(owner);
        setNonceKey(nonceKey);
        long currentTime = EnvironmentEdgeManager.currentTime();
        setSubmittedTime(currentTime);
        setLastUpdate(currentTime);
        setFailure(Objects.toString(exception.getMessage(), ""), exception);
    }

    @Override
    public String getProcName() {
        return procName;
    }

    @Override
    protected Procedure<TEnvironment>[] execute(TEnvironment env)
            throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void rollback(TEnvironment env) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(TEnvironment env) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
}
