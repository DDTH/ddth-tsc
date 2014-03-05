package com.github.ddth.tsc;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract implementation of {@link ICounterFactory}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class AbstractCounterFactory implements ICounterFactory {

    private ConcurrentHashMap<String, ICounter> counters = new ConcurrentHashMap<String, ICounter>();

    /**
     * {@inheritDoc}
     */
    @Override
    public ICounter getCounter(String name) {
        ICounter counter = counters.get(name);
        if (counter == null) {
            counter = createCounter(name);
            counters.putIfAbsent(name, counter);
        }
        return counter;
    }

    /**
     * Creates a new counter instance.
     * 
     * @param name
     * @return
     */
    protected abstract ICounter createCounter(String name);
}
