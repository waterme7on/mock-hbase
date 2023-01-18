package org.waterme7on.hbase.procedure2;

import java.io.IOException;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

public interface ProcedureStateSerializer {
    void serialize(Message message) throws IOException;

    <M extends Message> M deserialize(Class<M> clazz) throws IOException;
}
