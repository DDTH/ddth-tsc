package com.github.ddth.tsc.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.ICounter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * This factory creates {@link RedisCounter} instances.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class RedisCounterFactory extends AbstractCounterFactory {

    public final static int DEFAULT_TTL_SECONDS = 24 * 3600; // 1 day
    private final static long DEFAULT_TIMEOUT_MS = 10000; // 10 seconds

    private final Logger LOGGER = LoggerFactory.getLogger(RedisCounterFactory.class);

    /**
     * Creates a new {@link JedisPool}, with default database and timeout.
     * 
     * @param hostAndPort
     * @param password
     * @return
     * @sincec 0.7.0
     */
    public static JedisPool newJedisPool(String hostAndPort, String password) {
        return newJedisPool(hostAndPort, password, Protocol.DEFAULT_DATABASE, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Creates a new {@link JedisPool}, with specified database and default
     * timeout.
     * 
     * @param hostAndPort
     * @param password
     * @param db
     * @return
     * @since 0.7.0
     */
    public static JedisPool newJedisPool(String hostAndPort, String password, int db) {
        return newJedisPool(hostAndPort, password, db, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Creates a new {@link JedisPool} with default database and specified
     * timeout.
     * 
     * @param hostAndPort
     * @param password
     * @param timeoutMs
     * @return
     * @since 0.7.0
     */
    public static JedisPool newJedisPool(String hostAndPort, String password, long timeoutMs) {
        return newJedisPool(hostAndPort, password, Protocol.DEFAULT_DATABASE, timeoutMs);
    }

    /**
     * Creates a new {@link JedisPool}.
     * 
     * @param hostAndPort
     * @param password
     * @param db
     * @param timeoutMs
     * @return
     * @since 0.7.0
     */
    public static JedisPool newJedisPool(String hostAndPort, String password, int db,
            long timeoutMs) {
        final int maxTotal = Runtime.getRuntime().availableProcessors();
        final int maxIdle = maxTotal / 2;

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxIdle(maxIdle > 0 ? maxIdle : 1);
        poolConfig.setMaxWaitMillis(timeoutMs);
        // poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);

        String[] tokens = hostAndPort.split(":");
        String host = tokens.length > 0 ? tokens[0] : Protocol.DEFAULT_HOST;
        int port = tokens.length > 1 ? Integer.parseInt(tokens[1]) : Protocol.DEFAULT_PORT;
        JedisPool jedisPool = new JedisPool(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT,
                password, db);
        return jedisPool;
    }

    private JedisPool jedisPool;
    private boolean myOwnJedisPool = true;
    private String redisHostAndPort = Protocol.DEFAULT_HOST + ":" + Protocol.DEFAULT_PORT;
    private String redisPassword;
    private int ttlSeconds = DEFAULT_TTL_SECONDS;

    /**
     * Redis' host and port scheme (format {@code host:port}).
     * 
     * @return
     * @since 0.7.0
     */
    public String getRedisHostAndPort() {
        return redisHostAndPort;
    }

    /**
     * Sets Redis' host and port scheme (format {@code host:port}).
     * 
     * @param redisHostAndPort
     * @return
     * @since 0.7.0
     */
    public RedisCounterFactory setRedisHostAndPort(String redisHostAndPort) {
        this.redisHostAndPort = redisHostAndPort;
        return this;
    }

    /**
     * Redis' password.
     * 
     * @return
     * @since 0.7.0
     */
    public String getRedisPassword() {
        return redisPassword;
    }

    /**
     * Sets Redis' password.
     * 
     * @param redisPassword
     * @return
     * @since 0.7.0
     */
    public RedisCounterFactory setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
        return this;
    }

    /**
     * @return
     * @since 0.7.0
     */
    protected JedisPool getJedisPool() {
        return jedisPool;
    }

    /**
     * @param jedisPool
     * @return
     * @since 0.7.0
     */
    public RedisCounterFactory setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        myOwnJedisPool = false;
        return this;
    }

    public int getTtl() {
        return ttlSeconds;
    }

    public RedisCounterFactory setTtl(int ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RedisCounterFactory init() {
        if (jedisPool == null) {
            jedisPool = newJedisPool(redisHostAndPort, redisPassword);
            myOwnJedisPool = true;
        }
        super.init();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        try {
            super.destroy();
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }

        if (jedisPool != null && myOwnJedisPool) {
            try {
                jedisPool.destroy();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                jedisPool = null;
            }
        }
    }

    /**
     * @return
     * @since 0.7.0
     */
    public Jedis getJedis() {
        return jedisPool.getResource();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ICounter createCounter(String name) {
        RedisCounter counter = new RedisCounter(name, ttlSeconds);
        counter.setCounterFactory(this).init();
        return counter;
    }
}
