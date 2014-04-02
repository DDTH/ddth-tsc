package com.github.ddth.tsc.cassandra.internal;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Counter metadata manager.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class MetadataManager {

    public final static String DEFAULT_METADATA_TABLE = "tsc_metadata";
    private final static String DEFAULT_METADATA_NAME = "*";

    private final static String[] EMPTY_STRING_ARR = new String[0];
    private final static String CQL_GET_METADATA = "SELECT * FROM {0} WHERE c=?";

    private final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);

    private List<String> hosts = new ArrayList<String>();
    private String keyspace, tableMetadata = DEFAULT_METADATA_TABLE;
    private int port = 9042;
    private boolean myOwnCluster = false;
    private Cluster cluster;
    private Session session;

    private String cqlGetMetadata;

    public MetadataManager addHost(String host) {
        hosts.add(host);
        return this;
    }

    public String getHost() {
        return hosts.size() > 0 ? hosts.get(0) : null;
    }

    public MetadataManager setHost(String host) {
        hosts.clear();
        hosts.add(host);
        return this;
    }

    public Collection<String> getHosts() {
        return this.hosts;
    }

    public MetadataManager setHosts(Collection<String> hosts) {
        this.hosts.clear();
        if (hosts != null) {
            this.hosts.addAll(hosts);
        }
        return this;
    }

    public MetadataManager setHosts(String[] hosts) {
        this.hosts.clear();
        if (hosts != null) {
            for (String host : hosts) {
                this.hosts.add(host);
            }
        }
        return this;
    }

    public int getPort() {
        return port;
    }

    public MetadataManager setPort(int port) {
        this.port = port;
        return this;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public MetadataManager setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Name of the table to store counter metadata.
     * 
     * @param tableMetadata
     * @return
     */
    public MetadataManager setTableMetadata(String tableMetadata) {
        this.tableMetadata = tableMetadata;
        return this;
    }

    public String getTableMetadata() {
        return tableMetadata;
    }

    public MetadataManager setCluster(Cluster cluster) {
        if (session != null) {
            session.close();
            session = null;
        }
        if (this.cluster != null && myOwnCluster) {
            this.cluster.close();
        }
        this.cluster = cluster;
        myOwnCluster = false;

        return this;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void init() {
        int numProcessors = Runtime.getRuntime().availableProcessors();
        cache = CacheBuilder.newBuilder().concurrencyLevel(numProcessors)
                .expireAfterWrite(5, TimeUnit.MINUTES).build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        return _read(key);
                    }
                });

        if (cluster == null) {
            cluster = Cluster.builder().addContactPoints(hosts.toArray(EMPTY_STRING_ARR))
                    .withPort(port).build();
            myOwnCluster = true;
        }
        session = cluster.connect(keyspace);

        cqlGetMetadata = MessageFormat.format(CQL_GET_METADATA, tableMetadata);
    }

    public void destroy() {
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                session = null;
            }
        }

        if (cluster != null && myOwnCluster) {
            try {
                cluster.close();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                cluster = null;
            }
        }

        cache.invalidateAll();
    }

    /*----------------------------------------------------------------------*/
    private LoadingCache<String, String> cache;

    private String _read(String rowKey) throws RowNotFoundException {
        Row row = CassandraUtils.executeOne(session, cqlGetMetadata, rowKey);
        String jsonString = row != null ? row.getString("o") : null;
        if (jsonString != null) {
            return jsonString;
        }
        throw new RowNotFoundException(rowKey);
    }

    private String getRow(String rowKey) {
        try {
            return cache.get(rowKey);
        } catch (Exception e) {
            if (e.getCause() instanceof RowNotFoundException) {
                return null;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private CounterMetadata findCounterMetadata(String counterName) {
        String jsonString = getRow(DEFAULT_METADATA_NAME);
        List<Object> parent = SerializationUtils.fromJsonString(jsonString, List.class);
        if (parent == null) {
            return null;
        }
        for (Object entry : parent) {
            if (entry instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) entry;
                String pattern = DPathUtils.getValue(map, "pattern", String.class);
                if (Pattern.matches(pattern, counterName)) {
                    return CounterMetadata.fromMap(map);
                }
            }
        }
        return null;
    }

    /**
     * Gets a counter metadata by name.
     * 
     * @param name
     * @return
     */
    public CounterMetadata getCounterMetadata(String counterName) {
        String jsonString = getRow(counterName);
        if (jsonString != null) {
            CounterMetadata result = CounterMetadata.fromJsonString(jsonString);
            if (result != null) {
                result.name = counterName;
            }
            return result;
        } else {
            return findCounterMetadata(counterName);
        }
    }
    /*----------------------------------------------------------------------*/
}
