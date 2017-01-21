package com.github.ddth.tsc.redis;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.ICounter;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * This factory creates {@link ShardedRedisCounter} instances.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class ShardedRedisCounterFactory extends AbstractCounterFactory {

    public final static int DEFAULT_TTL_SECONDS = 24 * 3600; // 1 day
    private final static long DEFAULT_TIMEOUT_MS = 10000; // 10 seconds

    private final Logger LOGGER = LoggerFactory.getLogger(ShardedRedisCounterFactory.class);

    /**
     * Creates a new {@link ShardedJedisPool}, with default timeout.
     * 
     * @param hostsAndPorts
     *            format {@code host1:port1,host2:port2...}
     * @param password
     * @return
     */
    public static ShardedJedisPool newJedisPool(String hostsAndPorts, String password) {
        return newJedisPool(hostsAndPorts, password, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Creates a new {@link ShardedJedisPool}.
     * 
     * @param hostsAndPorts
     *            format {@code host1:port1,host2:port2...}
     * @param password
     * @param timeoutMs
     * @return
     */
    public static ShardedJedisPool newJedisPool(String hostsAndPorts, String password,
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

        List<JedisShardInfo> shards = new ArrayList<>();
        String[] hapList = hostsAndPorts.split("[,;\\s]+");
        for (String hostAndPort : hapList) {
            String[] tokens = hostAndPort.split(":");
            String host = tokens.length > 0 ? tokens[0] : Protocol.DEFAULT_HOST;
            int port = tokens.length > 1 ? Integer.parseInt(tokens[1]) : Protocol.DEFAULT_PORT;
            JedisShardInfo shardInfo = new JedisShardInfo(host, port);
            shardInfo.setPassword(password);
            shards.add(shardInfo);
        }
        ShardedJedisPool jedisPool = new ShardedJedisPool(poolConfig, shards);

        return jedisPool;
    }

    private ShardedJedisPool jedisPool;
    private boolean myOwnJedisPool = true;
    private String redisHostsAndPorts = Protocol.DEFAULT_HOST + ":" + Protocol.DEFAULT_PORT;
    private String redisPassword;
    private int ttlSeconds = DEFAULT_TTL_SECONDS;

    /**
     * Redis' hosts and ports scheme (format
     * {@code host1:port1,host2:port2,host3:port3}).
     * 
     * @return
     */
    public String getRedisHostsAndPorts() {
        return redisHostsAndPorts;
    }

    /**
     * Redis' hosts and ports scheme (format
     * {@code host1:port1,host2:port2,host3:port3}).
     * 
     * @param redisHostsAndPorts
     * @return
     */
    public ShardedRedisCounterFactory setRedisHostsAndPorts(String redisHostsAndPorts) {
        this.redisHostsAndPorts = redisHostsAndPorts;
        return this;
    }

    /**
     * Redis' password.
     * 
     * @return
     */
    public String getRedisPassword() {
        return redisPassword;
    }

    /**
     * Sets Redis' password.
     * 
     * @param redisPassword
     * @return
     */
    public ShardedRedisCounterFactory setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
        return this;
    }

    /**
     * @return
     */
    protected ShardedJedisPool getJedisPool() {
        return jedisPool;
    }

    /**
     * @param jedisPool
     * @return
     */
    public ShardedRedisCounterFactory setJedisPool(ShardedJedisPool jedisPool) {
        this.jedisPool = jedisPool;
        myOwnJedisPool = false;
        return this;
    }

    public int getTtl() {
        return ttlSeconds;
    }

    public ShardedRedisCounterFactory setTtl(int ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ShardedRedisCounterFactory init() {
        if (jedisPool == null) {
            jedisPool = newJedisPool(redisHostsAndPorts, redisPassword);
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
     */
    public ShardedJedis getJedis() {
        return jedisPool.getResource();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ICounter createCounter(String name) {
        ShardedRedisCounter counter = new ShardedRedisCounter(name, ttlSeconds);
        counter.setCounterFactory(this).init();
        return counter;
    }
}
