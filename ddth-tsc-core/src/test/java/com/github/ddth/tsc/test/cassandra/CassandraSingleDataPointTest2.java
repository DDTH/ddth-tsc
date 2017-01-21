package com.github.ddth.tsc.test.cassandra;

import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.cassandra.CassandraCounter;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test cases for {@link CassandraCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class CassandraSingleDataPointTest2 extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public CassandraSingleDataPointTest2(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(CassandraSingleDataPointTest2.class);
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

}
