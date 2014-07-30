package com.github.ddth.tsc.mem;

import java.util.Arrays;

import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;
import com.google.common.util.concurrent.AtomicLongMap;

/**
 * In-memory time series counter.
 * 
 * <ul>
 * <li>Support both {@code add()} and {@code get()} operators.</li>
 * <li>No persistent.</li>
 * <li>Store historical data for about 1 day (~86400 data points)</li>
 * </ul>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class InmemCounter extends AbstractCounter {

    private final static int DEFAULT_MAX_NUM_BLOCKS = 86400;
    private final static Long[] EMPTY_LONG_ARRAY = new Long[0];
    private final static int DEFAULT_BUFFER_NUM_BLOCKS = 20;

    private int BUFFER_NUM_BLOCKS = DEFAULT_BUFFER_NUM_BLOCKS;
    private int maxNumBlocks = DEFAULT_MAX_NUM_BLOCKS;
    private AtomicLongMap<Long> counter = AtomicLongMap.create();

    public InmemCounter() {
    }

    public InmemCounter(String name) {
        super(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init() {
        super.init();
        BUFFER_NUM_BLOCKS = Math.max(DEFAULT_MAX_NUM_BLOCKS / 10, DEFAULT_BUFFER_NUM_BLOCKS);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.4.2
     */
    @Override
    public InmemCounterFactory getCounterFactory() {
        return (InmemCounterFactory) super.getCounterFactory();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.4.2
     */
    @Override
    public InmemCounter setCounterFactory(AbstractCounterFactory counterFactory) {
        if (counterFactory instanceof InmemCounterFactory) {
            super.setCounterFactory(counterFactory);
        } else {
            throw new IllegalArgumentException("Argument must be an instance of "
                    + InmemCounterFactory.class.getName());
        }
        return this;
    }

    /*----------------------------------------------------------------------*/

    private void reduce() {
        Long[] keys = counter.asMap().keySet().toArray(EMPTY_LONG_ARRAY);
        Arrays.sort(keys);
        for (int i = 0, n = keys.length - maxNumBlocks + BUFFER_NUM_BLOCKS; i < n; i++) {
            counter.remove(keys[i]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(long timestampMs, long value) {
        Long key = toTimeSeriesPoint(timestampMs);
        counter.addAndGet(key, value);
        if (counter.size() > maxNumBlocks) {
            reduce();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(long timestampMs, long value) {
        Long key = toTimeSeriesPoint(timestampMs);
        counter.put(key, value);
        if (counter.size() > maxNumBlocks) {
            reduce();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint get(long timestampMs) {
        Long key = toTimeSeriesPoint(timestampMs);
        if (counter.containsKey(key)) {
            long value = counter.get(key);
            return new DataPoint(Type.SUM, key.longValue(), value, RESOLUTION_MS);
        } else {
            return new DataPoint(Type.NONE, key.longValue(), 0, RESOLUTION_MS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint get(long timestampMs, DataPoint.Type type, int steps) {
        int blockSize = steps * RESOLUTION_MS;
        Long key = toTimeSeriesPoint(timestampMs, steps);
        DataPoint result = new DataPoint().type(type).blockSize(blockSize)
                .timestamp(key.longValue());
        long _key = key.longValue();
        for (int i = 0; i < steps; i++) {
            if (counter.containsKey(_key)) {
                long value = counter.get(_key);
                result.add(value);
            }
            _key += RESOLUTION_MS;
        }
        return result;
    }
}
