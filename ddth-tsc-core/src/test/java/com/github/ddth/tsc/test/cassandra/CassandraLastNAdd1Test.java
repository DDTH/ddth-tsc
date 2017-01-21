package com.github.ddth.tsc.test.cassandra;

import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;
import com.github.ddth.tsc.cassandra.CassandraCounter;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test cases for {@link CassandraCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class CassandraLastNAdd1Test extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public CassandraLastNAdd1Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(CassandraLastNAdd1Test.class);
    }

    @org.junit.Test
    public void testLastNAdd1() throws InterruptedException {
        final long VALUE = 7;
        final int NUM_LOOP = 1000;
        final int NUM_THREAD = 4;

        Thread[] threads = new Thread[NUM_THREAD];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_LOOP; i++) {
                        counterAdd.add(VALUE);
                        try {
                            Thread.sleep(0);
                        } catch (InterruptedException e) {
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
        DataPoint[] dataPoints = counterAdd.getLastN(100);
        // number of points must match
        assertEquals(100, dataPoints.length);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            if (dp.type() != Type.NONE) {
                value += dp.value();
            }
        }
        // sum value must match
        // assuming the backend is fast enough (last 100 points contain enough
        // data!)
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, value);
    }
}
