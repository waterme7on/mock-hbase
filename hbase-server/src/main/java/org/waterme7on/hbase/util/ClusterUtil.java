package org.waterme7on.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.waterme7on.hbase.master.HMaster;
import org.waterme7on.hbase.regionserver.HRegion;
import org.waterme7on.hbase.regionserver.HRegionServer;
import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

public class ClusterUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterUtil.class);

    public static class RegionServerThread extends Thread {
        private final HRegionServer regionServer;

        public RegionServerThread(final HRegionServer r, final int index) {
            super(r, "RS:" + index + ";" + r.getServerName().toShortString());
            this.regionServer = r;
        }

        /** Returns the region server */
        public HRegionServer getRegionServer() {
            return this.regionServer;
        }

        /**
         * Block until the region server has come online, indicating it is ready to be
         * used.
         */
        public void waitForServerOnline() {
            // The server is marked online after the init method completes inside of
            // the HRS#run method. HRS#init can fail for whatever region. In those
            // cases, we'll jump out of the run without setting online flag. Check
            // stopRequested so we don't wait here a flag that will never be flipped.
            // regionServer.waitForServerOnline();
        }
    }

    public static class MasterThread extends Thread {
        private final HMaster master;

        public MasterThread(final HMaster m, final int index) {
            super(m, "Master:" + index + ";" + m.getServerName().toShortString());
            this.master = m;
        }

        public HMaster getMaster() {
            return this.master;
        }
    }

    /**
     * Creates a {@link RegionServerThread}. Call 'start' on the returned thread to
     * make it run.
     * 
     * @param c     Configuration to use.
     * @param hrsc  Class to create.
     * @param index Used distinguishing the object returned.
     * @return Region server added.
     */
    public static RegionServerThread createRegionServerThread(final Configuration c,
            final Class<? extends HRegionServer> hrsc, final int index) throws IOException {
        HRegionServer server;
        try {
            Constructor<? extends HRegionServer> ctor = hrsc.getConstructor(Configuration.class);
            ctor.setAccessible(true);
            server = ctor.newInstance(c);
        } catch (InvocationTargetException ite) {
            Throwable target = ite.getTargetException();
            throw new RuntimeException("Failed construction of RegionServer: " + hrsc.toString()
                    + ((target.getCause() != null) ? target.getCause().getMessage() : ""), target);
        } catch (Exception e) {
            throw new IOException(e);
        }
        return new RegionServerThread(server, index);
    }

    /**
     * Creates a {@link MasterThread}. Call 'start' on the returned thread to make
     * it run.
     * 
     * @param c     Configuration to use.
     * @param hmc   Class to create.
     * @param index Used distinguishing the object returned.
     * @return Master added.
     */
    public static MasterThread createMasterThread(final Configuration c,
            final Class<? extends HMaster> hmc, final int index) throws IOException {
        HMaster server;
        try {
            server = hmc.getConstructor(Configuration.class).newInstance(c);
        } catch (InvocationTargetException ite) {
            Throwable target = ite.getTargetException();
            throw new RuntimeException("Failed construction of Master: " + hmc.toString()
                    + ((target.getCause() != null) ? target.getCause().getMessage() : ""), target);
        } catch (Exception e) {
            throw new IOException(e);
        }
        // Needed if a master based registry is configured for internal cluster
        // connections. Here, we
        // just add the current master host port since we do not know other master
        // addresses up front
        // in mini cluster tests.
        // TODO

        return new MasterThread(server, index);
    }

    /**
     * Start the cluster. Waits until there is a primary master initialized and
     * returns its address.
     * 
     * @return Address to use contacting primary master.
     */
    public static String startup(final List<MasterThread> masters,
            final List<RegionServerThread> regionservers) throws IOException {
        LOG.info("Starting HBase Cluster {} masters, {} region servers", masters.size(),
                regionservers.size());
        Configuration configuration = null;

        if (masters == null || masters.isEmpty()) {
            return null;
        }

        for (MasterThread t : masters) {
            configuration = t.getMaster().getConfiguration();
            LOG.info("Starting master thread: {}", t.getName());
            t.start();
        }

        // Wait for an active master
        // having an active master before starting the region threads allows
        // then to succeed on their connection to master
        final int startTimeout = configuration != null
                ? Integer.parseInt(configuration.get("hbase.master.start.timeout.localHBaseCluster", "30000"))
                : 30000;
        waitForEvent(startTimeout, "active", () -> findActiveMaster(masters) != null);

        if (regionservers != null) {
            for (RegionServerThread t : regionservers) {
                LOG.info("Starting region server thread: {}", t.getName());
                t.start();
            }
        }

        return findActiveMaster(masters).master.getServerName().toString();
    }

    public static void shutdown(final List<MasterThread> masters,
            final List<RegionServerThread> regionservers) {
        LOG.info("Shutting down HBase Cluster");
        if (masters != null) {
            // Do backups first.
            MasterThread activeMaster = null;
            for (MasterThread t : masters) {
                // Master was killed but could be still considered as active. Check first if it
                // is stopped.
                if (!t.master.isStopped()) {
                    if (!t.master.isActiveMaster()) {
                        try {
                            t.master.stopMaster();
                        } catch (IOException e) {
                            LOG.error("Exception occurred while stopping master", e);
                        }
                        LOG.info("Stopped backup Master {} is stopped: {}", t.master.hashCode(),
                                t.master.isStopped());
                    } else {
                        if (activeMaster != null) {
                            LOG.warn("Found more than 1 active master, hash {}", activeMaster.master.hashCode());
                        }
                        activeMaster = t;
                        LOG.debug("Found active master hash={}, stopped={}", t.master.hashCode(),
                                t.master.isStopped());
                    }
                }
            }
            // Do active after.
            if (activeMaster != null) {
                try {
                    activeMaster.master.shutdown();
                } catch (IOException e) {
                    LOG.error("Exception occurred in HMaster.shutdown()", e);
                }
            }
        }
        boolean wasInterrupted = false;
        final long maxTime = EnvironmentEdgeManager.currentTime() + 30 * 1000;
        if (regionservers != null) {
            // first try nicely.
            for (RegionServerThread t : regionservers) {
                t.getRegionServer().stop("Shutdown requested");
            }
            for (RegionServerThread t : regionservers) {
                long now = EnvironmentEdgeManager.currentTime();
                if (t.isAlive() && !wasInterrupted && now < maxTime) {
                    try {
                        t.join(maxTime - now);
                    } catch (InterruptedException e) {
                        LOG.info("Got InterruptedException on shutdown - "
                                + "not waiting anymore on region server ends", e);
                        wasInterrupted = true; // someone wants us to speed up.
                    }
                }
            }

            // Let's try to interrupt the remaining threads if any.
            for (int i = 0; i < 100; ++i) {
                boolean atLeastOneLiveServer = false;
                for (RegionServerThread t : regionservers) {
                    if (t.isAlive()) {
                        atLeastOneLiveServer = true;
                        try {
                            LOG.warn("RegionServerThreads remaining, give one more chance before interrupting");
                            t.join(1000);
                        } catch (InterruptedException e) {
                            wasInterrupted = true;
                        }
                    }
                }
                if (!atLeastOneLiveServer)
                    break;
                for (RegionServerThread t : regionservers) {
                    if (t.isAlive()) {
                        LOG.warn("RegionServerThreads taking too long to stop, interrupting; thread dump "
                                + "if > 3 attempts: i=" + i);
                        if (i > 3) {
                            Threads.printThreadInfo(System.out, "Thread dump " + t.getName());
                        }
                        t.interrupt();
                    }
                }
            }
        }

        if (masters != null) {
            for (MasterThread t : masters) {
                while (t.master.isAlive() && !wasInterrupted) {
                    try {
                        // The below has been replaced to debug sometime hangs on end of
                        // tests.
                        // this.master.join():
                        Threads.threadDumpingIsAlive(t.master);
                    } catch (InterruptedException e) {
                        LOG.info(
                                "Got InterruptedException on shutdown - " + "not waiting anymore on master ends", e);
                        wasInterrupted = true;
                    }
                }
            }
        }
        LOG.info("Shutdown of " + ((masters != null) ? masters.size() : "0") + " master(s) and "
                + ((regionservers != null) ? regionservers.size() : "0") + " regionserver(s) "
                + (wasInterrupted ? "interrupted" : "complete"));

        if (wasInterrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static MasterThread findActiveMaster(List<MasterThread> masters) {
        for (MasterThread t : masters) {
            if (t.master.isActiveMaster()) {
                return t;
            }
        }

        return null;
    }

    /**
     * Utility method to wait some time for an event to occur, and then return
     * control to the caller.
     * 
     * @param millis How long to wait, in milliseconds.
     * @param action The action that we are waiting for. Will be used in log message
     *               if the event does
     *               not occur.
     * @param check  A Supplier that will be checked periodically to produce an
     *               updated true/false
     *               result indicating if the expected event has happened or not.
     * @throws InterruptedIOException If we are interrupted while waiting for the
     *                                event.
     * @throws RuntimeException       If we reach the specified timeout while
     *                                waiting for the event.
     */
    private static void waitForEvent(long millis, String action, Supplier<Boolean> check)
            throws InterruptedIOException {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);

        while (true) {
            if (check.get()) {
                return;
            }

            if (System.nanoTime() > end) {
                String msg = "Master not " + action + " after " + millis + "ms";
                Threads.printThreadInfo(System.out, "Thread dump because: " + msg);
                throw new RuntimeException(msg);
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw (InterruptedIOException) new InterruptedIOException().initCause(e);
            }
        }

    }

}
