package com.github.ddth.tsc.redis;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.mem.InmemCounter;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class SingleDataPointSet2Test extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public SingleDataPointSet2Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(SingleDataPointSet2Test.class);
    }

    @org.junit.Test
    public void testSingleDataPoint2() {
        final long VALUE = 3;
        final int NUM_LOOP = 1000;

        long timestamp = System.currentTimeMillis();
        long delta = timestamp % AbstractCounter.RESOLUTION_MS;
        long key = timestamp - delta;
        for (int i = 0; i < NUM_LOOP; i++) {
            counter2.set(timestamp, VALUE + i);
        }
        DataPoint dataPoint = counter2.get(timestamp);
        assertEquals(VALUE + NUM_LOOP - 1, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }
}
