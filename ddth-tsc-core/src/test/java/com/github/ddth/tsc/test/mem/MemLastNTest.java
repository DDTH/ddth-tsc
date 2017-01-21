package com.github.ddth.tsc.test.mem;

import com.github.ddth.tsc.ICounterFactory;
import com.github.ddth.tsc.mem.InmemCounter;
import com.github.ddth.tsc.mem.InmemCounterFactory;
import com.github.ddth.tsc.test.BaseLastNTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class MemLastNTest extends BaseLastNTest {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public MemLastNTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(MemLastNTest.class);
    }

    @Override
    protected ICounterFactory createCounterFactory() {
        return new InmemCounterFactory().init();
    }

}
