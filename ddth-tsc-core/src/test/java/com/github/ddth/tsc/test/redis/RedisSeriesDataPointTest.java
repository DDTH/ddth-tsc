package com.github.ddth.tsc.test.redis;

import org.junit.After;

import com.github.ddth.tsc.ICounterFactory;
import com.github.ddth.tsc.redis.RedisCounter;
import com.github.ddth.tsc.redis.RedisCounterFactory;
import com.github.ddth.tsc.test.BaseSeriesDataPointTest;

import junit.framework.Test;
import junit.framework.TestSuite;
import redis.embedded.RedisServer;

/**
 * Test cases for {@link RedisCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class RedisSeriesDataPointTest extends BaseSeriesDataPointTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public RedisSeriesDataPointTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(RedisSeriesDataPointTest.class);
    }

    protected RedisServer redisServer;
    private final static String REDIS_HOST = "127.0.0.1";
    private final static int REDIS_PORT = 16379;
    private final static String REDIS_HOST_AND_PORT = REDIS_HOST + ":" + REDIS_PORT;

    @Override
    protected ICounterFactory createCounterFactory() throws Exception {
        try {
            redisServer = new RedisServer(REDIS_PORT);
            redisServer.start();
            return new RedisCounterFactory().setRedisHostAndPort(REDIS_HOST_AND_PORT).init();
        } catch (Exception e) {
            tearDown();
            throw e;
        }
    }

    @After
    public void tearDown() {
        if (redisServer != null) {
            try {
                redisServer.stop();
            } catch (Exception e) {
            } finally {
                redisServer = null;
            }
        }
        super.tearDown();
    }
}
