package org.waterme7on.hbase.wal;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.waterme7on.hbase.Server;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.asyncfs.monitor.ExcludeDatanodeManager;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.ReaderBase;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.waterme7on.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WALFactory {

    private static final Logger LOG = LoggerFactory.getLogger(WALFactory.class);
    private static final String SINGLETON_ID = WALFactory.class.getName();

    /**
     * Maps between configuration names for providers and implementation classes.
     */
    enum Providers {
        defaultProvider(AsyncFSWALProvider.class);
        // filesystem(FSHLogProvider.class),
        // multiwal(RegionGroupingProvider.class),
        // asyncfs(AsyncFSWALProvider.class);

        final Class<? extends WALProvider> clazz;

        Providers(Class<? extends WALProvider> clazz) {
            this.clazz = clazz;
        }
    }

    private final WALProvider provider;
    /**
     * Configuration-specified WAL Reader used when a custom reader is requested
     */
    private final Class<? extends ReaderBase> logReaderClass;
    final Abortable abortable;
    /**
     * How long to attempt opening in-recovery wals
     */
    private final int timeoutMillis;

    private final Configuration conf;
    private final ExcludeDatanodeManager excludeDatanodeManager;
    public static final String WAL_PROVIDER = "hbase.wal.provider";
    static final String DEFAULT_WAL_PROVIDER = Providers.defaultProvider.name();

    public static final String META_WAL_PROVIDER = "hbase.wal.meta_provider";

    public static final String WAL_ENABLED = "hbase.regionserver.hlog.enabled";
    final String factoryId;

    public WALFactory(Configuration conf) throws IOException {
        // this code is duplicated here so we can keep our members final.
        // until we've moved reader/writer construction down into providers, this
        // initialization must
        // happen prior to provider initialization, in case they need to instantiate a
        // reader/writer.
        timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);
        /* TODO Both of these are probably specific to the fs wal provider */
        logReaderClass = ProtobufLogReader.class;
        this.conf = conf;
        // this instance can't create wals, just reader/writers.
        provider = null;
        this.excludeDatanodeManager = new ExcludeDatanodeManager(conf);
        factoryId = SINGLETON_ID;
        this.abortable = null;
    }

    public WALFactory(Configuration conf, String factoryId) throws IOException {
        this(conf, factoryId, null);
    }

    /*
     * @param
     * conf: must not be null, will keep a reference to read params in later
     * reader/writer instances.
     * 
     * abortable: the server to abort
     */
    public WALFactory(Configuration conf, String factoryId, Abortable abortable) throws IOException {
        // until we've moved reader/writer construction down into providers, this
        // initialization must
        // happen prior to provider initialization, in case they need to instantiate a
        // reader/writer.
        timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);
        /* TODO Both of these are probably specific to the fs wal provider */
        logReaderClass = ProtobufLogReader.class;
        this.conf = conf;
        this.factoryId = factoryId;
        this.abortable = abortable;
        this.excludeDatanodeManager = new ExcludeDatanodeManager(conf);
        // end required early initialization
        if (conf.getBoolean(WAL_ENABLED, true)) {
            provider = getProvider(WAL_PROVIDER, DEFAULT_WAL_PROVIDER, null);
        } else {
            // special handling of existing configuration behavior.
            LOG.warn("Running with WAL disabled.");
            provider = new DisabledWALProvider();
            provider.init(this, conf, factoryId, null);
        }
    }

    public WAL getWAL(RegionInfo regionInfo) throws IOException {
        return provider.getWAL(regionInfo);
    }

    /**
     * ##############################
     * Helper Functions
     * ##############################
     */

    Providers getDefaultProvider() {
        return Providers.defaultProvider;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends WALProvider> getProviderClass(String key, String defaultValue) {
        try {
            Providers provider = Providers.valueOf(conf.get(key, defaultValue));

            provider = getDefaultProvider();
            return provider.clazz;
        } catch (IllegalArgumentException exception) {
            return conf.getClass(key, Providers.defaultProvider.clazz, WALProvider.class);
        }
    }

    WALProvider createProvider(Class<? extends WALProvider> clazz, String providerId)
            throws IOException {
        LOG.info("Instantiating WALProvider of type " + clazz);
        try {
            final WALProvider result = clazz.getDeclaredConstructor().newInstance();
            result.init(this, conf, providerId, this.abortable);
            return result;
        } catch (Exception e) {
            LOG.error("couldn't set up WALProvider, the configured class is " + clazz);
            LOG.debug("Exception details for failure to load WALProvider.", e);
            throw new IOException("couldn't set up WALProvider", e);
        }
    }

    /**
     * instantiate a provider from a config property. requires conf to have already
     * been set (as well
     * as anything the provider might need to read).
     */
    WALProvider getProvider(String key, String defaultValue, String providerId) throws IOException {
        Class<? extends WALProvider> clazz = getProviderClass(key, defaultValue);
        WALProvider provider = createProvider(clazz, providerId);
        provider.addWALActionsListener(new MetricsWAL());
        return provider;
    }

    public static Reader createReader(FileSystem fileSystem, Path path, Configuration conf2) {
        return null;
    }

    private static final AtomicReference<WALFactory> singleton = new AtomicReference<>();

    // Public only for FSHLog
    public static WALFactory getInstance(Configuration configuration) throws IOException {
        WALFactory factory = singleton.get();
        if (null == factory) {
            WALFactory temp = new WALFactory(configuration);
            if (singleton.compareAndSet(null, temp)) {
                factory = temp;
            } else {
                // someone else beat us to initializing
                try {
                    temp.close();
                } catch (IOException exception) {
                    LOG.debug("failed to close temporary singleton. ignoring.", exception);
                }
                factory = singleton.get();
            }
        }
        return factory;
    }

    /**
     * Shutdown all WALs and clean up any underlying storage. Use only when you will
     * not need to
     * replay and edits that have gone to any wals from this factory.
     */
    public void close() throws IOException {
        // close is called on a WALFactory with null provider in the case of contention
        // handling
        // within the getInstance method.
        if (null != provider) {
            provider.close();
        }
    }

    public ExcludeDatanodeManager getExcludeDatanodeManager() {
        return excludeDatanodeManager;
    }
}
