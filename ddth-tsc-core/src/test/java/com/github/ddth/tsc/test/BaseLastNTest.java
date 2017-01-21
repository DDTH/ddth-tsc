package com.github.ddth.tsc.test;

import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class BaseLastNTest extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public BaseLastNTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(BaseLastNTest.class);
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
        assertEquals(0, dataPoints[0].value());
        assertEquals(0, dataPoints[1].value());
        assertEquals(0, dataPoints[2].value());
        assertEquals(0, dataPoints[3].value());
        assertEquals(0, dataPoints[4].value());
        assertEquals(0, dataPoints[5].value());
        assertEquals(0, dataPoints[6].value());
        assertEquals(0, dataPoints[7].value());
        assertEquals(0, dataPoints[8].value());
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, dataPoints[9].value());
    }
}
