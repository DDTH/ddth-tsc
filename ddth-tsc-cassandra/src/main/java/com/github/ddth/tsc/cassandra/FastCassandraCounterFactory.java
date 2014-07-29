package com.github.ddth.tsc.cassandra;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.tsc.ICounter;
import com.github.ddth.tsc.cassandra.internal.CounterMetadata;

/**
 * This factory creates {@link FastCassandraCounter} instances.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class FastCassandraCounterFactory extends CassandraCounterFactory {

    private final Logger LOGGER = LoggerFactory.getLogger(FastCassandraCounterFactory.class);

    private int queueSize = 50;
    private int tickDuration = 1000;
    private ScheduledExecutorService executorService;

    protected int getQueueSize() {
        return queueSize;
    }

    public FastCassandraCounterFactory setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    protected int getTickDuration() {
        return tickDuration;
    }

    public FastCassandraCounterFactory setTickDuration(int tickDuration) {
        this.tickDuration = tickDuration;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FastCassandraCounterFactory init() {
        Runtime runtime = Runtime.getRuntime();
        executorService = Executors.newScheduledThreadPool(runtime.availableProcessors() * 2);
        return (FastCassandraCounterFactory) super.init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        try {
            executorService.shutdown();
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }

        try {
            super.destroy();
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ICounter createCounter(String name) {
        CounterMetadata metadata = getCounterMetadata(name);
        if (metadata == null) {
            throw new IllegalStateException("No metadata found for counter [" + name + "]!");
        }

        FastCassandraCounter counter = new FastCassandraCounter();
        counter.setName(name);
        counter.setSession(getSession());
        counter.setMetadata(metadata);
        counter.setQueueSize(queueSize);
        counter.setTickDuration(tickDuration);
        counter.setExecutorService(executorService);
        counter.init();
        return counter;
    }

}
