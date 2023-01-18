package org.waterme7on.hbase.coprocessor;

import java.util.Optional;
import org.apache.hadoop.hbase.Coprocessor;

public interface RegionCoprocessor extends Coprocessor {

    // default Optional<RegionObserver> getRegionObserver() {
    // return Optional.empty();
    // }

    // default Optional<EndpointObserver> getEndpointObserver() {
    // return Optional.empty();
    // }

    // default Optional<BulkLoadObserver> getBulkLoadObserver() {
    // return Optional.empty();
    // }
}
