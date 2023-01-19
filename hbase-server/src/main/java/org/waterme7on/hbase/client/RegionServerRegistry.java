package org.waterme7on.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ConnectionRegistry;
import org.waterme7on.hbase.regionserver.HRegionServer;

/**
 * Connection registry implementation for region server.
 */
public class RegionServerRegistry implements ConnectionRegistry {

    private final HRegionServer regionServer;

    public RegionServerRegistry(HRegionServer regionServer) {
        this.regionServer = regionServer;
    }

    @Override
    public CompletableFuture<RegionLocations> getMetaRegionLocations() {
        CompletableFuture<RegionLocations> future = new CompletableFuture<>();
        Optional<List<HRegionLocation>> locs =
                regionServer.getMetaRegionLocationCache().getMetaRegionLocations();
        if (locs.isPresent()) {
            List<HRegionLocation> list = locs.get();
            if (list.isEmpty()) {
                future.completeExceptionally(new IOException("no meta location available"));
            } else {
                future.complete(new RegionLocations(list));
            }
        } else {
            future.completeExceptionally(new IOException("no meta location available"));
        }
        return future;
    }

    @Override
    public CompletableFuture<String> getClusterId() {
        return CompletableFuture.completedFuture(regionServer.getClusterId());
    }

    @Override
    public CompletableFuture<ServerName> getActiveMaster() {
        CompletableFuture<ServerName> future = new CompletableFuture<>();
        Optional<ServerName> activeMaster = regionServer.getActiveMaster();
        if (activeMaster.isPresent()) {
            future.complete(activeMaster.get());
        } else {
            future.completeExceptionally(new IOException("no active master available"));
        }
        return future;
    }

    @Override
    public String getConnectionString() {
        return "short-circuit";
    }

    @Override
    public void close() {
        // nothing
    }
}
