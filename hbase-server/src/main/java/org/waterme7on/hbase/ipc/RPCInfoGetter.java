package org.waterme7on.hbase.ipc;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.shaded.protobuf.generated.TracingProtos;

/**
 * Used to extract a tracing {@link Context} from an instance of
 * {@link TracingProtos.RPCTInfo}.
 */
final class RPCTInfoGetter implements TextMapGetter<TracingProtos.RPCTInfo> {
    RPCTInfoGetter() {
    }

    @Override
    public Iterable<String> keys(TracingProtos.RPCTInfo carrier) {
        return Optional.ofNullable(carrier).map(TracingProtos.RPCTInfo::getHeadersMap).map(Map::keySet)
                .orElse(Collections.emptySet());
    }

    @Override
    public String get(TracingProtos.RPCTInfo carrier, String key) {
        return Optional.ofNullable(carrier).map(TracingProtos.RPCTInfo::getHeadersMap)
                .map(map -> map.get(key)).orElse(null);
    }
}
