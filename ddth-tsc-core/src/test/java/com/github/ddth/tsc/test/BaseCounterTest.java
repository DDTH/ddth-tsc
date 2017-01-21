package com.github.ddth.tsc.test;

import org.junit.After;
import org.junit.Before;

import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.ICounter;
import com.github.ddth.tsc.ICounterFactory;
import com.github.ddth.tsc.cassandra.internal.CqlTemplate;

import junit.framework.TestCase;

/**
 * Base class for test cases.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class BaseCounterTest extends TestCase {

    protected ICounter counterAdd, counterSet;
    protected ICounterFactory counterFactory;

    protected final static String COUNTER_NAME_ADD = CqlTemplate.TABLE_COUNTER + "_add";
    protected final static String COUNTER_NAME_SET = CqlTemplate.TABLE_COUNTER + "_set";

    public BaseCounterTest(String testName) {
        super(testName);
    }

    protected abstract ICounterFactory createCounterFactory() throws Exception;

    @Before
    public void setUp() throws Exception {
        counterFactory = createCounterFactory();
        counterAdd = counterFactory.getCounter(COUNTER_NAME_ADD);
        counterSet = counterFactory.getCounter(COUNTER_NAME_SET);
    }

    @After
    public void tearDown() {
        if (counterFactory instanceof AbstractCounterFactory) {
            ((AbstractCounterFactory) counterFactory).destroy();
            counterFactory = null;
        }
    }

}
