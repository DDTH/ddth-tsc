package com.github.ddth.tsc;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.github.ddth.tsc.mem.InmemCounter;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class InmemCounterTest extends TestCase {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public InmemCounterTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(InmemCounterTest.class);
    }

    @org.junit.Test
    public void testSingleDataPoint() {
        final long VALUE = 2;

        ICounter counter = new InmemCounter("demo");
        long timestamp = System.currentTimeMillis();
        long delta = timestamp % AbstractCounter.RESOLUTION_MS;
        long key = timestamp - delta;
        counter.add(timestamp, VALUE);
        DataPoint dataPoint = counter.get(timestamp);
        assertEquals(VALUE, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }

    @org.junit.Test
    public void testSingleDataPoint2() {
        final long VALUE = 3;
        final int NUM_LOOP = 1000;

        ICounter counter = new InmemCounter("demo");
        long timestamp = System.currentTimeMillis();
        long delta = timestamp % AbstractCounter.RESOLUTION_MS;
        long key = timestamp - delta;
        for (int i = 0; i < NUM_LOOP; i++) {
            counter.add(timestamp, VALUE);
        }
        DataPoint dataPoint = counter.get(timestamp);
        assertEquals(VALUE * NUM_LOOP, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }

    @org.junit.Test
    public void testSingleDataPoint3() throws InterruptedException {
        final long VALUE = 5;
        final int NUM_LOOP = 1000;
        final int NUM_THREAD = 4;
        final ICounter counter = new InmemCounter("demo");
        final long timestamp = System.currentTimeMillis();

        Thread[] threads = new Thread[NUM_THREAD];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_LOOP; i++) {
                        counter.add(timestamp, VALUE);
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
        DataPoint dataPoint = counter.get(timestamp);
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }

    @org.junit.Test
    public void testSeriesDataPoints() throws InterruptedException {
        final long VALUE = 7;
        final int NUM_LOOP = 1000;
        final int NUM_THREAD = 4;
        final ICounter counter = new InmemCounter("demo");

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
        final ICounter counter = new InmemCounter("demo");

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
        final ICounter counter = new InmemCounter("demo");

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
