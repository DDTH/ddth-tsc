package com.github.ddth.tsc.test;

import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.DataPoint;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class BaseSingleDataPointTest extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public BaseSingleDataPointTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(BaseSingleDataPointTest.class);
    }

    @org.junit.Test
    public void testSingleDataPoint1() {
        final long VALUE = 2;

        long timestamp = System.currentTimeMillis();
        long delta = timestamp % AbstractCounter.RESOLUTION_MS;
        long key = timestamp - delta;
        counterAdd.add(timestamp, VALUE);
        DataPoint dataPoint = counterAdd.get(timestamp);
        assertEquals(VALUE, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }

    @org.junit.Test
    public void testSingleDataPoint2() {
        final long VALUE = 3;
        final int NUM_LOOP = 1000;

        long timestamp = System.currentTimeMillis();
        long delta = timestamp % AbstractCounter.RESOLUTION_MS;
        long key = timestamp - delta;
        for (int i = 0; i < NUM_LOOP; i++) {
            counterAdd.add(timestamp, VALUE);
        }
        DataPoint dataPoint = counterAdd.get(timestamp);
        assertEquals(VALUE * NUM_LOOP, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
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
                        counterAdd.add(timestamp, VALUE);
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
        DataPoint dataPoint = counterAdd.get(timestamp);
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }
}
