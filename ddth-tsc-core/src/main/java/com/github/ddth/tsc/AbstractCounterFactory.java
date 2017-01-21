package com.github.ddth.tsc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Abstract implementation of {@link ICounterFactory}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class AbstractCounterFactory implements ICounterFactory {

    private LoadingCache<String, ICounter> counterCache;

    /**
     * Initializing method.
     * 
     * @return
     * @since 0.1.1
     */
    public AbstractCounterFactory init() {
        int numProcessors = Runtime.getRuntime().availableProcessors();
        counterCache = CacheBuilder.newBuilder().concurrencyLevel(Math.max(numProcessors, 8))
                .expireAfterAccess(3600, TimeUnit.SECONDS)
                .removalListener(new RemovalListener<String, ICounter>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, ICounter> notification) {
                        ICounter counter = notification.getValue();
                        destroyCounter(counter);
                    }
                }).build(new CacheLoader<String, ICounter>() {
                    @Override
                    public ICounter load(String key) throws Exception {
                        return createCounter(key);
                    }
                });
        return this;
    }

    /**
     * Destroying method.
     * 
     * @since 0.1.1
     */
    public void destroy() {
        if (counterCache != null) {
            counterCache.invalidateAll();
            counterCache = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICounter getCounter(String name) {
        try {
            return counterCache.get(name);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new counter instance.
     * 
     * @param name
     * @return
     */
    protected abstract ICounter createCounter(String name);

    /**
     * Destroys and removes a counter from the cache.
     * 
     * @param counter
     * @since 0.2.0
     */
    protected void destroyCounter(ICounter counter) {
        try {
            if (counter instanceof AbstractCounter) {
                ((AbstractCounter) counter).destroy();
            }
        } catch (Exception e) {
        }
    }
}
