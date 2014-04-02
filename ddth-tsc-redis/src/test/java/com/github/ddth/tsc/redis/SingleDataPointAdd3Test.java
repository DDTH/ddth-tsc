package com.github.ddth.tsc.redis;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.redis.RedisCounter;

/**
 * Test cases for {@link RedisCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class SingleDataPointAdd3Test extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public SingleDataPointAdd3Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(SingleDataPointAdd3Test.class);
    }

    @org.junit.Test
    public void testSingleDataPoint3() throws InterruptedException {
        final long VALUE = 5;
        final int NUM_LOOP = 1000;
        final int NUM_THREAD = 4;
        final long timestamp = System.currentTimeMillis();

        Thread[] threads = new Thread[NUM_THREAD];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_LOOP; i++) {
                        counter1.add(timestamp, VALUE);
                        try {
                            Thread.sleep(0);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };
        }

        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        long delta = timestamp % AbstractCounter.RESOLUTION_MS;
        long key = timestamp - delta;
        DataPoint dataPoint = counter1.get(timestamp);
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }
}
