package com.github.ddth.tsc.cassandra;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.DataPoint;

/**
 * Test cases for {@link CassandraCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class SingleDataPointSet3Test extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public SingleDataPointSet3Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(SingleDataPointSet3Test.class);
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
                        counter2.set(timestamp, VALUE + i);
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
        DataPoint dataPoint = counter2.get(timestamp);
        assertEquals(VALUE + NUM_LOOP - 1, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }
}
