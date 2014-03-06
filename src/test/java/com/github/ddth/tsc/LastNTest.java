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
public class LastNTest extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public LastNTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(LastNTest.class);
    }

    @org.junit.Test
    public void testLastN() throws InterruptedException {
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
        DataPoint[] dataPoints = counter.getLastN(10);
        assertEquals(10, dataPoints.length);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, value);
    }

    @org.junit.Test
    public void testLastN2() throws InterruptedException {
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
        DataPoint[] dataPoints = counter.getLastN(10, 60);
        assertEquals(10, dataPoints.length);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, value);

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
