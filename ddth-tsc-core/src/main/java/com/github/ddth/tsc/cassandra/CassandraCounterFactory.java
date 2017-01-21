package com.github.ddth.tsc.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.ICounter;
import com.github.ddth.tsc.cassandra.internal.CounterMetadata;
import com.github.ddth.tsc.cassandra.internal.MetadataManager;
import com.github.ddth.tsc.cassandra.internal.SessionHelper;

/**
 * This factory creates {@link CassandraCounter} instances.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.2.0
 */
public class CassandraCounterFactory extends AbstractCounterFactory {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraCounterFactory.class);

    /*
     * Cassandra hosts & ports, username and password See:
     * https://github.com/DDTH/ddth-cql-utils
     */
    private String hostsAndPorts, username, password;
    private String keyspace, tableMetadata = MetadataManager.DEFAULT_METADATA_TABLE;
    private SessionManager sessionManager;
    private ConsistencyLevel consistencyLevelForRead = ConsistencyLevel.LOCAL_ONE;
    private ConsistencyLevel consistencyLevelForWrite = ConsistencyLevel.LOCAL_ONE;
    private ConsistencyLevel consistencyLevelForReadForUpdate = ConsistencyLevel.LOCAL_ONE;

    private MetadataManager metadataManager;
    private ICacheFactory cacheFactory;
    private SessionHelper helper;

    /**
     * Hosts & Ports to connect to Cassandra cluster.
     * 
     * @return
     * @since 0.6.0
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    public String getHostsAndPorts() {
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
    public CassandraCounterFactory setHostsAndPorts(String hostsAndPorts) {
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
    public String getUsername() {
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
    public CassandraCounterFactory setUsername(String username) {
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
    public String getPassword() {
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
    public CassandraCounterFactory setPassword(String password) {
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
    public SessionManager getSessionManager() {
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
    public CassandraCounterFactory setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        return this;
    }

    /**
     * {@link ConsistencyLevel} for read operations.
     * 
     * @return
     * @since 0.7.0
     */
    public ConsistencyLevel getConsistencyLevelForRead() {
        return consistencyLevelForRead;
    }

    /**
     * Sets {@link ConsistencyLevel} for read operations.
     * 
     * @param consistencyLevelForRead
     * @since 0.7.0
     */
    public CassandraCounterFactory setConsistencyLevelForRead(
            ConsistencyLevel consistencyLevelForRead) {
        this.consistencyLevelForRead = consistencyLevelForRead;
        return this;
    }

    /**
     * {@link ConsistencyLevel} for write operations.
     * 
     * @return
     * @since 0.7.0
     */
    public ConsistencyLevel getConsistencyLevelForWrite() {
        return consistencyLevelForWrite;
    }

    /**
     * Sets {@link ConsistencyLevel} for write operations.
     * 
     * @param consistencyLevelForWrite
     * @since 0.7.0
     */
    public CassandraCounterFactory setConsistencyLevelForWrite(
            ConsistencyLevel consistencyLevelForWrite) {
        this.consistencyLevelForWrite = consistencyLevelForWrite;
        return this;
    }

    /**
     * {@link ConsistencyLevel} for read-for-update operations.
     * 
     * @return
     * @since 0.7.0
     */
    public ConsistencyLevel getConsistencyLevelForReadForUpdate() {
        return consistencyLevelForReadForUpdate;
    }

    /**
     * Sets {@link ConsistencyLevel} for read-for-update operations.
     * 
     * @param consistencyLevelForReadForUpdate
     * @return
     */
    public CassandraCounterFactory setConsistencyLevelForReadForUpdate(
            ConsistencyLevel consistencyLevelForReadForUpdate) {
        this.consistencyLevelForReadForUpdate = consistencyLevelForReadForUpdate;
        return this;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public CassandraCounterFactory setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    public String getTableMetadata() {
        return tableMetadata;
    }

    public CassandraCounterFactory setTableMetadata(String tableMetadata) {
        this.tableMetadata = tableMetadata;
        return this;
    }

    public ICacheFactory getCacheFactory() {
        return cacheFactory;
    }

    public CassandraCounterFactory setCacheFactory(ICacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CassandraCounterFactory init() {
        this.helper = new SessionHelper(sessionManager, hostsAndPorts, username, password,
                keyspace);
        helper.setCacheFactory(cacheFactory);

        this.metadataManager = new MetadataManager(helper).init();

        return (CassandraCounterFactory) super.init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        if (metadataManager != null) {
            try {
                metadataManager.destroy();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                metadataManager = null;
            }
        }
    }

    /**
     * Returns Cassandra session instance.
     * 
     * @return
     * @since 0.4.1
     */
    protected Session getSession() {
        return sessionManager.getSession(hostsAndPorts, username, password, keyspace);
    }

    /**
     * Gets counter's metadata.
     * 
     * @param name
     * @return
     * @since 0.4.1
     */
    protected CounterMetadata getCounterMetadata(String name) {
        return metadataManager.getCounterMetadata(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ICounter createCounter(String name) {
        CounterMetadata metadata = getCounterMetadata(name);
        if (metadata == null) {
            throw new IllegalStateException("No metadata found for counter [" + name + "]!");
        }
        CassandraCounter counter = new CassandraCounter(helper, name, metadata);
        counter.setCounterFactory(this);
        counter.init();
        return counter;
    }

}
