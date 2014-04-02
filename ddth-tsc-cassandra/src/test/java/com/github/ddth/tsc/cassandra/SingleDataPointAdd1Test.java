package com.github.ddth.tsc.cassandra;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.mem.InmemCounter;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class SingleDataPointAdd1Test extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public SingleDataPointAdd1Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(SingleDataPointAdd1Test.class);
    }

    @org.junit.Test
    public void testSingleDataPoint1() {
        final long VALUE = 2;

        long timestamp = System.currentTimeMillis();
        long delta = timestamp % AbstractCounter.RESOLUTION_MS;
        long key = timestamp - delta;
        counter1.add(timestamp, VALUE);
        DataPoint dataPoint = counter1.get(timestamp);
        assertEquals(VALUE, dataPoint.value());
        assertEquals(key, dataPoint.timestamp());
    }
}
