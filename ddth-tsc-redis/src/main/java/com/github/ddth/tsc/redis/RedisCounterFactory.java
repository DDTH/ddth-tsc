package com.github.ddth.tsc.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.redis.IRedisClient;
import com.github.ddth.redis.PoolConfig;
import com.github.ddth.redis.RedisClientFactory;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.ICounter;

/**
 * This factory creates {@link RedisCounter} instances.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class RedisCounterFactory extends AbstractCounterFactory {

    // private final static String[] EMPTY_STRING_ARR = new String[0];

    public final static int DEFAULT_TTL_SECONDS = 24 * 3600; // 1 day

    private final Logger LOGGER = LoggerFactory.getLogger(RedisCounterFactory.class);

    private String host;
    private int port = IRedisClient.DEFAULT_REDIS_PORT;
    private String username, password;
    private RedisClientFactory redisClientFactory;
    private boolean myOwnFactory = false;
    private PoolConfig poolConfig;
    private int ttlSeconds = DEFAULT_TTL_SECONDS;

    public String getHost() {
        return host;
    }

    public RedisCounterFactory setHost(String host) {
        this.host = host;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public RedisCounterFactory setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public RedisCounterFactory setPassword(String password) {
        this.password = password;
        return this;
    }

    public int getPort() {
        return port;
    }

    public RedisCounterFactory setPort(int port) {
        this.port = port;
        return this;
    }

    public PoolConfig getRedisPoolConfig() {
        return poolConfig;
    }

    public RedisCounterFactory setRedisPoolConfig(PoolConfig poolConfig) {
        this.poolConfig = poolConfig;
        return this;
    }

    public RedisCounterFactory setRedisClientFactory(RedisClientFactory redisClientFactory) {
        if (this.redisClientFactory != null && myOwnFactory) {
            this.redisClientFactory.destroy();
        }
        this.redisClientFactory = redisClientFactory;
        myOwnFactory = false;

        return this;
    }

    public RedisClientFactory getRedisClientFactory() {
        return redisClientFactory;
    }

    public int getTTL() {
        return ttlSeconds;
    }

    public RedisCounterFactory setTTL(int ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
        return this;
    }

    protected IRedisClient getRedisClient() {
        return redisClientFactory.getRedisClient(host, port, username, password, poolConfig);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RedisCounterFactory init() {
        if (redisClientFactory == null) {
            redisClientFactory = RedisClientFactory.newFactory();
            myOwnFactory = true;
        }
        return (RedisCounterFactory) super.init();
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

        if (redisClientFactory != null && myOwnFactory) {
            try {
                redisClientFactory.destroy();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                redisClientFactory = null;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ICounter createCounter(String name) {
        RedisCounter counter = new RedisCounter(name, this, ttlSeconds);
        counter.init();
        return counter;
    }
}
