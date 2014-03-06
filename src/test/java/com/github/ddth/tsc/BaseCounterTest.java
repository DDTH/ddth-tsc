package com.github.ddth.tsc;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.github.ddth.tsc.mem.InmemCounter;
import com.github.ddth.tsc.mem.InmemCounterFactory;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class BaseCounterTest extends TestCase {

    protected InmemCounter counter;
    protected InmemCounterFactory counterFactory;

    public BaseCounterTest(String testName) {
        super(testName);
    }

    @Before
    public void setUp() {
        counterFactory = new InmemCounterFactory();
        counterFactory.init();

        counter = (InmemCounter) counterFactory.getCounter("demo");
    }

    @After
    public void tearDown() {
        if (counterFactory != null) {
            counterFactory.destroy();
            counterFactory = null;
        }

        if (counter != null) {
            counter.destroy();
            counter = null;
        }
    }
}
