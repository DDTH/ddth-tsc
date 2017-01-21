package com.github.ddth.tsc.cassandra.internal;

import com.datastax.driver.core.Session;
import com.github.ddth.cacheadapter.ICache;
import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.cql.SessionManager;

/**
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class SessionHelper {
    /*
     * Cassandra hosts & ports, username and password See:
     * https://github.com/DDTH/ddth-cql-utils
     */
    private String hostsAndPorts, username, password;
    private String keyspace;
    private SessionManager sessionManager;
    private ICacheFactory cacheFactory;

    public SessionHelper() {
    }

    public SessionHelper(SessionManager sessionManager, String hostsAndPorts, String username,
            String password, String keyspace) {
        this.sessionManager = sessionManager;
        this.hostsAndPorts = hostsAndPorts;
        this.username = username;
        this.password = password;
        this.keyspace = keyspace;
    }

    /**
     * Hosts & Ports to connect to Cassandra cluster.
     * 
     * @return
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
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    public SessionHelper setHostsAndPorts(String hostsAndPorts) {
        this.hostsAndPorts = hostsAndPorts;
        return this;
    }

    /**
     * Username to connect to Cassandra cluster.
     * 
     * @return
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
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    public SessionHelper setUsername(String username) {
        this.username = username;
        return this;
    }

    /**
     * Password to connect to Cassandra cluster.
     * 
     * @return
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
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    public SessionHelper setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public SessionHelper setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Cassandra session manager.
     * 
     * @return
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
     * @see https://github.com/DDTH/ddth-cql-utils
     */
    public SessionHelper setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        return this;
    }

    public ICacheFactory getCacheFactory() {
        return cacheFactory;
    }

    public SessionHelper setCacheFactory(ICacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
        return this;
    }

    /*----------------------------------------------------------------------*/

    public ICache getCache(String name) {
        return cacheFactory != null ? cacheFactory.createCache(name) : null;
    }

    public Session getSession() {
        return sessionManager.getSession(hostsAndPorts, username, password, keyspace);
    }
}
