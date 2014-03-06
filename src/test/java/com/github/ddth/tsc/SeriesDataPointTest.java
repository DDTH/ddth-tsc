package com.github.ddth.tsc;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.github.ddth.tsc.mem.InmemCounter;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class SeriesDataPointTest extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public SeriesDataPointTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(SeriesDataPointTest.class);
    }

    @org.junit.Test
    public void testSeriesDataPoints() throws InterruptedException {
        final long VALUE = 7;
        final int NUM_LOOP = 1000;
        final int NUM_THREAD = 4;

        Thread[] threads = new Thread[NUM_THREAD];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_LOOP; i++) {
                        counter.add(VALUE);
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
        DataPoint[] dataPoints = counter.getSeries(timestampStart, timestampEnd);
        assertTrue(dataPoints.length >= 1);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
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
                        counter.add(VALUE);
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
        DataPoint[] dataPoints = counter.getSeries(timestampStart, timestampEnd, 2);
        assertTrue(dataPoints.length >= 1);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
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
                        counter.add(VALUE);
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
        DataPoint[] dataPoints = counter.getSeries(timestampStart, timestampEnd, 7);
        assertTrue(dataPoints.length >= 1);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, value);
    }
}
