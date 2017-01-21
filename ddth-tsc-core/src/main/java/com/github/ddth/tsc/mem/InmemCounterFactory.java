package com.github.ddth.tsc.mem;

import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.ICounter;

/**
 * This factory creates {@link InmemCounter} instances.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class InmemCounterFactory extends AbstractCounterFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    protected ICounter createCounter(String name) {
        InmemCounter counter = new InmemCounter(name);
        counter.setCounterFactory(this).init();
        return counter;
    }

}
