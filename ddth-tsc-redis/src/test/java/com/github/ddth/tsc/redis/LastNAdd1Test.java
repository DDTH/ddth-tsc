package com.github.ddth.tsc.redis;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.mem.InmemCounter;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class LastNAdd1Test extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public LastNAdd1Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(LastNAdd1Test.class);
    }

    @org.junit.Test
    public void testLastN1() throws InterruptedException {
        final long VALUE = 7;
        final int NUM_LOOP = 1000;
        final int NUM_THREAD = 4;

        Thread[] threads = new Thread[NUM_THREAD];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_LOOP; i++) {
                        counter1.add(VALUE);
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
        DataPoint[] dataPoints = counter1.getLastN(10);
        assertEquals(10, dataPoints.length);

        long value = 0;
        for (DataPoint dp : dataPoints) {
            value += dp.value();
        }
        assertEquals(VALUE * NUM_LOOP * NUM_THREAD, value);
    }
}
