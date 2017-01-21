package com.github.ddth.tsc.test.shardedredis;

import org.junit.After;

import com.github.ddth.tsc.ICounterFactory;
import com.github.ddth.tsc.redis.ShardedRedisCounter;
import com.github.ddth.tsc.redis.ShardedRedisCounterFactory;
import com.github.ddth.tsc.test.BaseLastNTest;

import junit.framework.Test;
import junit.framework.TestSuite;
import redis.embedded.RedisServer;

/**
 * Test cases for {@link ShardedRedisCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class ShardedRedisLastNTest extends BaseLastNTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public ShardedRedisLastNTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(ShardedRedisLastNTest.class);
    }

    protected RedisServer redisServer1, redisServer2;
    private final static String REDIS_HOST = "127.0.0.1";
    private final static int REDIS_PORT1 = 16379;
    private final static int REDIS_PORT2 = 16380;
    private final static String REDIS_HOSTS_AND_PORTS = REDIS_HOST + ":" + REDIS_PORT1 + ","
            + REDIS_HOST + ":" + REDIS_PORT2;

    @Override
    protected ICounterFactory createCounterFactory() throws Exception {
        try {
            redisServer1 = new RedisServer(REDIS_PORT1);
            redisServer1.start();
            redisServer2 = new RedisServer(REDIS_PORT2);
            redisServer2.start();
            return new ShardedRedisCounterFactory().setRedisHostsAndPorts(REDIS_HOSTS_AND_PORTS)
                    .init();
        } catch (Exception e) {
            tearDown();
            throw e;
        }
    }

    @After
    public void tearDown() {
        if (redisServer1 != null) {
            try {
                redisServer1.stop();
            } catch (Exception e) {
            } finally {
                redisServer1 = null;
            }
        }
        if (redisServer2 != null) {
            try {
                redisServer2.stop();
            } catch (Exception e) {
            } finally {
                redisServer2 = null;
            }
        }
        super.tearDown();
    }

}
