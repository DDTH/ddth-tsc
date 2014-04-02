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
public class SingleDataPointSet1Test extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public SingleDataPointSet1Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(SingleDataPointSet1Test.class);
    }

    @org.junit.Test
    public void testSingleDataPoint1() {
        final long VALUE = 2;

        long timestamp = System.currentTimeMillis();
        long delta = timestamp % AbstractCounter.RESOLUTION_MS;
        long key = timestamp - delta;
        counter2.set(timestamp, VALUE);
        DataPoint dataPoint = counter2.get(timestamp);
        assertEquals(VALUE, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }
}
