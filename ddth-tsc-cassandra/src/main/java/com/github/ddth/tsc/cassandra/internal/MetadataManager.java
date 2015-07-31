package com.github.ddth.tsc.cassandra.internal;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;
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

    private final static String CQL_GET_METADATA = "SELECT * FROM {0} WHERE c=?";

    /*
     * Cassandra hosts & ports, username and password See:
     * https://github.com/DDTH/ddth-cql-utils
     */
    private String hostsAndPorts, username, password;
    private String keyspace, tableMetadata = MetadataManager.DEFAULT_METADATA_TABLE;
    private SessionManager sessionManager;

    private String cqlGetMetadata;

    /**
     * Hosts & Ports to connect to Cassandra cluster.
     * 
     * @return
     * @since 0.6.0
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    protected String getHostsAndPorts() {
        return hostsAndPorts;
    }

    /**
     * Hosts & Ports to connect to Cassandra cluster.
     * 
     * @param hostsAndPorts
     * @return
     * @since 0.6.0
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    public MetadataManager setHostsAndPorts(String hostsAndPorts) {
        this.hostsAndPorts = hostsAndPorts;
        return this;
    }

    /**
     * Username to connect to Cassandra cluster.
     * 
     * @return
     * @since 0.6.0
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    protected String getUsername() {
        return username;
    }

    /**
     * Username to connect to Cassandra cluster.
     * 
     * @param username
     * @return
     * @since 0.6.0
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    public MetadataManager setUsername(String username) {
        this.username = username;
        return this;
    }

    /**
     * Password to connect to Cassandra cluster.
     * 
     * @return
     * @since 0.6.0
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    protected String getPassword() {
        return password;
    }

    /**
     * Password to connect to Cassandra cluster.
     * 
     * @param password
     * @return
     * @since 0.6.0
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    public MetadataManager setPassword(String password) {
        this.password = password;
        return this;
    }

    /**
     * Cassandra session manager.
     * 
     * @return
     * @since 0.6.0
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    protected SessionManager getSessionManager() {
        return sessionManager;
    }

    /**
     * Cassandra session manager.
     * 
     * @param sessionManager
     * @return
     * @since 0.6.0
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    public MetadataManager setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        return this;
    }

    protected String getKeyspace() {
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

    protected String getTableMetadata() {
        return tableMetadata;
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
        cqlGetMetadata = MessageFormat.format(CQL_GET_METADATA, tableMetadata);
    }

    public void destroy() {
        cache.invalidateAll();
    }

    /*----------------------------------------------------------------------*/

    /**
     * Obtains a Cassandra session.
     * 
     * @return
     */
    private Session getSession() {
        return sessionManager.getSession(hostsAndPorts, username, password, keyspace);
    }

    private LoadingCache<String, String> cache;

    /**
     * Reads a row data from storage (no cache).
     * 
     * @param rowKey
     * @return
     * @throws RowNotFoundException
     */
    private String _read(String rowKey) throws RowNotFoundException {
        Row row = CqlUtils.executeOne(getSession(), cqlGetMetadata, ConsistencyLevel.LOCAL_QUORUM,
                rowKey);
        String jsonString = row != null ? row.getString("o") : null;
        if (jsonString != null) {
            return jsonString;
        }
        throw new RowNotFoundException(rowKey);
    }

    /**
     * Reads a row data from storage (cache supported).
     * 
     * @param rowKey
     * @return
     */
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
