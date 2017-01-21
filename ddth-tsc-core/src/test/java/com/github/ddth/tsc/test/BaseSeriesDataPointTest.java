package com.github.ddth.tsc.test;

import com.github.ddth.tsc.DataPoint;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class BaseSeriesDataPointTest extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public BaseSeriesDataPointTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(BaseSeriesDataPointTest.class);
    }

    @org.junit.Test
    public void testSeriesDataPoints1() throws InterruptedException {
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
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            };
        }

        long timestampStart = System.currentTimeMillis();
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        long timestampEnd = System.currentTimeMillis() + 1;
        DataPoint[] dataPoints = counterAdd.getSeries(timestampStart, timestampEnd);
        // min: 1 point
        assertTrue(dataPoints.length >= 1);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
        // sum value must match
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, value);
    }

    @org.junit.Test
    public void testSeriesDataPoints2() throws InterruptedException {
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
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            };
        }

        long timestampStart = System.currentTimeMillis();
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        long timestampEnd = System.currentTimeMillis() + 1;
        DataPoint[] dataPoints = counterAdd.getSeries(timestampStart, timestampEnd, 2);
        // min: 1 points
        assertTrue(dataPoints.length >= 1);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
        // sum value must match
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, value);
    }

    @org.junit.Test
    public void testSeriesDataPoints3() throws InterruptedException {
        final long VALUE = 13;
        final int NUM_LOOP = 5000;
        final int NUM_THREAD = 4;

        Thread[] threads = new Thread[NUM_THREAD];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_LOOP; i++) {
                        counterAdd.add(VALUE);
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            };
        }

        long timestampStart = System.currentTimeMillis();
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        long timestampEnd = System.currentTimeMillis() + 1;
        DataPoint[] dataPoints = counterAdd.getSeries(timestampStart, timestampEnd, 7);
        // min: 1 point
        assertTrue(dataPoints.length >= 1);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
        // sum value must match
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, value);
    }
}
