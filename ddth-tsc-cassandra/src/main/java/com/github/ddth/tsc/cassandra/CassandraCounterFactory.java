package com.github.ddth.tsc.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.ICounter;
import com.github.ddth.tsc.cassandra.internal.CounterMetadata;
import com.github.ddth.tsc.cassandra.internal.MetadataManager;

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
    private ICacheFactory cacheFactory;

    private MetadataManager metadataManager;

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
    public CassandraCounterFactory setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        return this;
    }

    protected ICacheFactory getCacheFactory() {
        return cacheFactory;
    }

    public CassandraCounterFactory setCacheFactory(ICacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
        return this;
    }

    protected String getKeyspace() {
        return keyspace;
    }

    public CassandraCounterFactory setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    protected String getTableMetadata() {
        return tableMetadata;
    }

    public CassandraCounterFactory setTableMetadata(String tableMetadata) {
        this.tableMetadata = tableMetadata;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CassandraCounterFactory init() {
        metadataManager = new MetadataManager();
        metadataManager.setHostsAndPorts(hostsAndPorts).setUsername(username).setPassword(password)
                .setKeyspace(keyspace).setTableMetadata(tableMetadata)
                .setSessionManager(sessionManager);
        metadataManager.init();

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

        CassandraCounter counter = new CassandraCounter();
        counter.setMetadata(metadata);
        counter.setName(name).setCounterFactory(this);
        counter.setHostsAndPorts(hostsAndPorts).setUsername(username).setPassword(password)
                .setKeyspace(keyspace).setSessionManager(sessionManager);
        counter.setCacheFactory(cacheFactory);
        counter.init();
        return counter;
    }

}
