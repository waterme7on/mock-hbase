package org.waterme7on.hbase.master;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ServerManager.class);
    /** Map of registered servers to their current load */
    private final ConcurrentNavigableMap<ServerName, ServerMetrics> onlineServers = new ConcurrentSkipListMap<>();
    private final RegionServerList storage;
    private final MasterServices master;
    /** Listeners that are called on server events. */
    private List<ServerListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Constructor.
     */
    public ServerManager(final MasterServices master, RegionServerList storage) {
        this.master = master;
        this.storage = storage;
        // Configuration c = master.getConfiguration();
        // this.connection = master.getClusterConnection();
        // this.rpcControllerFactory = this.connection == null ? null :
        // connection.getRpcControllerFactory();
    }

    void shutdownCluster() {
    };

    /**
     * Let the server manager know a new regionserver has come online
     * 
     * @param request       the startup request
     * @param versionNumber the version number of the new regionserver
     * @param version       the version of the new regionserver, could contain
     *                      strings like "SNAPSHOT"
     * @param ia            the InetAddress from which request is received
     * @return The ServerName we know this server as.
     */
    ServerName regionServerStartup(RegionServerStartupRequest request, int versionNumber,
            String version, InetAddress ia) throws IOException {
        // Test for case where we get a region startup message from a regionserver
        // that has been quickly restarted but whose znode expiration handler has
        // not yet run, or from a server whose fail we are currently processing.
        // Test its host+port combo is present in serverAddressToServerInfo. If it
        // is, reject the server and trigger its expiration. The next time it comes
        // in, it should have been removed from serverAddressToServerInfo and queued
        // for processing by ProcessServerShutdown.

        final String hostname = request.hasUseThisHostnameInstead() ? request.getUseThisHostnameInstead()
                : ia.getHostName();
        ServerName sn = ServerName.valueOf(hostname, request.getPort(), request.getServerStartCode());
        // checkClockSkew(sn, request.getServerCurrentTime());
        checkIsDead(sn, "STARTUP");
        if (!checkAndRecordNewServer(sn, ServerMetricsBuilder.of(sn, versionNumber, version))) {
            LOG.warn(
                    "THIS SHOULD NOT HAPPEN, RegionServerStartup" + " could not record the server: " + sn);
        }
        storage.started(sn);
        return sn;
    }

    /**
     * Check is a server of same host and port already exists, if not, or the
     * existed one got a
     * smaller start code, record it.
     * 
     * @param serverName the server to check and record
     * @param sl         the server load on the server
     * @return true if the server is recorded, otherwise, false
     */
    boolean checkAndRecordNewServer(final ServerName serverName, final ServerMetrics sl) {
        ServerName existingServer = null;
        synchronized (this.onlineServers) {
            existingServer = findServerWithSameHostnamePortWithLock(serverName);
            if (existingServer != null && (existingServer.getStartcode() > serverName.getStartcode())) {
                LOG.info("Server serverName=" + serverName + " rejected; we already have "
                        + existingServer.toString() + " registered with same hostname and port");
                return false;
            }
            recordNewServerWithLock(serverName, sl);
        }

        // Tell our listeners that a server was added
        if (!this.listeners.isEmpty()) {
            for (ServerListener listener : this.listeners) {
                listener.serverAdded(serverName);
            }
        }

        // Note that we assume that same ts means same server, and don't expire in that
        // case.
        // TODO: ts can theoretically collide due to clock shifts, so this is a bit
        // hacky.
        if (existingServer != null && (existingServer.getStartcode() < serverName.getStartcode())) {
            LOG.info("Triggering server recovery; existingServer " + existingServer
                    + " looks stale, new server:" + serverName);
            expireServer(existingServer);
        }
        return true;
    }

    private void checkIsDead(final ServerName serverName, final String what) {
        // TODO
    }

    /**
     * Expire the passed server. Add it to list of dead servers and queue a shutdown
     * processing.
     * 
     * @return pid if we queued a ServerCrashProcedure else
     *         {@link Procedure#NO_PROC_ID} if we did not
     *         (could happen for many reasons including the fact that its this
     *         server that is going
     *         down or we already have queued an SCP for this server or SCP
     *         processing is currently
     *         disabled because we are in startup phase).
     */
    // Redo test so we can make this protected.
    public synchronized long expireServer(final ServerName serverName) {
        // TODO
        // return expireServer(serverName, false);
        return 0;
    }

    /**
     * Assumes onlineServers is locked.
     * 
     * @return ServerName with matching hostname and port.
     */
    private ServerName findServerWithSameHostnamePortWithLock(final ServerName serverName) {
        ServerName end = ServerName.valueOf(serverName.getHostname(), serverName.getPort(), Long.MAX_VALUE);

        ServerName r = onlineServers.lowerKey(end);
        if (r != null) {
            if (ServerName.isSameAddress(r, serverName)) {
                return r;
            }
        }
        return null;
    }

    /**
     * Adds the onlineServers list. onlineServers should be locked.
     * 
     * @param serverName The remote servers name.
     */
    void recordNewServerWithLock(final ServerName serverName, final ServerMetrics sl) {
        LOG.info("Registering regionserver=" + serverName);
        this.onlineServers.put(serverName, sl);
        // this.rsAdmins.remove(serverName);
    }

    /** Returns the count of active regionservers */
    public int countOfRegionServers() {
        // Presumes onlineServers is a concurrent map
        return this.onlineServers.size();
    }

    /** Returns Read-only map of servers to serverinfo */
    public Map<ServerName, ServerMetrics> getOnlineServers() {
        // Presumption is that iterating the returned Map is OK.
        synchronized (this.onlineServers) {
            return Collections.unmodifiableMap(this.onlineServers);
        }
    }

    /** Returns A copy of the internal list of online servers. */
    public List<ServerName> getOnlineServersList() {
        // TODO: optimize the load balancer call so we don't need to make a new list
        // TODO: FIX. THIS IS POPULAR CALL.
        return new ArrayList<>(this.onlineServers.keySet());
    }

    public List<ServerName> createDestinationServersList() {
        return new ArrayList<>(this.onlineServers.keySet());
    }
}
