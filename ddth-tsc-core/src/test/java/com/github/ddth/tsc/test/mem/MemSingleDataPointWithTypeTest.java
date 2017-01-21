package com.github.ddth.tsc.test.mem;

import com.github.ddth.tsc.ICounterFactory;
import com.github.ddth.tsc.mem.InmemCounter;
import com.github.ddth.tsc.mem.InmemCounterFactory;
import com.github.ddth.tsc.test.BaseSingleDataPointWithTypeTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class MemSingleDataPointWithTypeTest extends BaseSingleDataPointWithTypeTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public MemSingleDataPointWithTypeTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(MemSingleDataPointWithTypeTest.class);
    }

    @Override
    protected ICounterFactory createCounterFactory() {
        return new InmemCounterFactory().init();
    }
}
