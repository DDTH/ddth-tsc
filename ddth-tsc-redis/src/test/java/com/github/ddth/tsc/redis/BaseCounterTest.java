package com.github.ddth.tsc.redis;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import redis.embedded.RedisServer;

import com.github.ddth.tsc.ICounter;
import com.github.ddth.tsc.mem.InmemCounter;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public abstract class BaseCounterTest extends TestCase {

    protected ICounter counter1, counter2;
    protected RedisCounterFactory counterFactory;
    protected RedisServer redisServer;

    private final static String REDIS_HOST = "127.0.0.1";
    private final static int REDIS_PORT = 16379;

    public BaseCounterTest(String testName) {
        super(testName);
    }

    @Before
    public void setUp() throws Exception {
        redisServer = new RedisServer(REDIS_PORT);
        redisServer.start();

        counterFactory = new RedisCounterFactory().setHost(REDIS_HOST).setPort(REDIS_PORT).init();
        counter1 = counterFactory.getCounter("test_counter_1");
        counter2 = counterFactory.getCounter("test_counter_2");
    }

    @After
    public void tearDown() throws Exception {
        if (counterFactory != null) {
            try {
                counterFactory.destroy();
            } catch (Exception e) {
            } finally {
                counterFactory = null;
            }
        }

        if (redisServer != null) {
            try {
                redisServer.stop();
            } catch (Exception e) {
            } finally {
                redisServer = null;
            }
        }
    }
}
