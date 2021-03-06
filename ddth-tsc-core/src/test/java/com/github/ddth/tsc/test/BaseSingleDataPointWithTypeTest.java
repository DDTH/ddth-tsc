package com.github.ddth.tsc.test;

import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;
import com.github.ddth.tsc.ICounter;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class BaseSingleDataPointWithTypeTest extends BaseCounterTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public BaseSingleDataPointWithTypeTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(BaseSingleDataPointWithTypeTest.class);
    }

    @org.junit.Test
    public void testSingleDataPoint1() {
        final long VALUE = 2;

        int steps = 7;
        int blockSize = steps * ICounter.RESOLUTION_MS;
        long timestamp = System.currentTimeMillis();
        long delta = timestamp % blockSize;
        long key = timestamp - delta;
        long timestamp1 = timestamp - delta + 1;
        long timestamp2 = timestamp1 + 2 * ICounter.RESOLUTION_MS;
        long timestamp3 = timestamp1 + 3 * ICounter.RESOLUTION_MS;
        counterAdd.add(timestamp1, VALUE);
        counterAdd.add(timestamp2, VALUE - 1);
        counterAdd.add(timestamp3, VALUE + 2);

        {
            DataPoint dataPoint = counterAdd.get(timestamp, Type.SUM, steps);
            assertEquals(Type.SUM, dataPoint.type());
            assertEquals(VALUE + VALUE - 1 + VALUE + 2, dataPoint.value());
            assertEquals(key, dataPoint.timestamp());
        }

        {
            DataPoint dataPoint = counterAdd.get(timestamp, Type.MINIMUM, steps);
            assertEquals(Type.MINIMUM, dataPoint.type());
            assertEquals(VALUE - 1, dataPoint.value());
            assertEquals(key, dataPoint.timestamp());
        }

        {
            DataPoint dataPoint = counterAdd.get(timestamp, Type.MAXIMUM, steps);
            assertEquals(Type.MAXIMUM, dataPoint.type());
            assertEquals(VALUE + 2, dataPoint.value());
            assertEquals(key, dataPoint.timestamp());
        }

        {
            DataPoint dataPoint = counterAdd.get(timestamp, Type.AVERAGE, steps);
            assertEquals(Type.AVERAGE, dataPoint.type());
            assertEquals((VALUE + VALUE - 1 + VALUE + 2) / 3, dataPoint.value());
            assertEquals(key, dataPoint.timestamp());
        }
    }

    @org.junit.Test
    public void testSingleDataPoint2() {
        final long VALUE = 2;

        int steps = 7;
        int blockSize = steps * ICounter.RESOLUTION_MS;
        long timestamp = System.currentTimeMillis();
        long delta = timestamp % blockSize;
        long key = timestamp - delta;
        long timestamp1 = timestamp - delta + 1;
        long timestamp2 = timestamp1 + 2 * ICounter.RESOLUTION_MS;
        long timestamp3 = timestamp1 + 3 * ICounter.RESOLUTION_MS;
        counterSet.set(timestamp1, VALUE);
        counterSet.set(timestamp2, VALUE - 1);
        counterSet.set(timestamp3, VALUE + 2);

        {
            DataPoint dataPoint = counterSet.get(timestamp, Type.SUM, steps);
            assertEquals(Type.SUM, dataPoint.type());
            assertEquals(VALUE + VALUE - 1 + VALUE + 2, dataPoint.value());
            assertEquals(key, dataPoint.timestamp());
        }

        {
            DataPoint dataPoint = counterSet.get(timestamp, Type.MINIMUM, steps);
            assertEquals(Type.MINIMUM, dataPoint.type());
            assertEquals(VALUE - 1, dataPoint.value());
            assertEquals(key, dataPoint.timestamp());
        }

        {
            DataPoint dataPoint = counterSet.get(timestamp, Type.MAXIMUM, steps);
            assertEquals(Type.MAXIMUM, dataPoint.type());
            assertEquals(VALUE + 2, dataPoint.value());
            assertEquals(key, dataPoint.timestamp());
        }

        {
            DataPoint dataPoint = counterSet.get(timestamp, Type.AVERAGE, steps);
            assertEquals(Type.AVERAGE, dataPoint.type());
            assertEquals((VALUE + VALUE - 1 + VALUE + 2) / 3, dataPoint.value());
            assertEquals(key, dataPoint.timestamp());
        }
    }
}
