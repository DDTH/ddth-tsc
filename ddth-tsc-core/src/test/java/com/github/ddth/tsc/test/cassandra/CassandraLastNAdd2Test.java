package com.github.ddth.tsc.test.cassandra;

import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.cassandra.CassandraCounter;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test cases for {@link CassandraCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class CassandraLastNAdd2Test extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public CassandraLastNAdd2Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(CassandraLastNAdd2Test.class);
    }

    @org.junit.Test
    public void testLastNAdd2() throws InterruptedException {
        final long VALUE = 11;
        final int NUM_LOOP = 5000;
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
        DataPoint[] dataPoints = counterAdd.getLastN(10, 60);
        // number of points must match
        assertEquals(10, dataPoints.length);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
        // sum value must match
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, value);

        // assuming step of 60 seconds is large enough so that all writes go
        // to the last block
        long total = 0;
        for (DataPoint dp : dataPoints) {
            total = dp.value();
        }
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, total);
        // assertEquals(0, dataPoints[0].value());
        // assertEquals(0, dataPoints[1].value());
        // assertEquals(0, dataPoints[2].value());
        // assertEquals(0, dataPoints[3].value());
        // assertEquals(0, dataPoints[4].value());
        // assertEquals(0, dataPoints[5].value());
        // assertEquals(0, dataPoints[6].value());
        // assertEquals(0, dataPoints[7].value());
        // assertEquals(0, dataPoints[8].value());
        // assertEquals(VALUE * NUM_LOOP * NUM_THREAD, dataPoints[9].value());
    }
}
