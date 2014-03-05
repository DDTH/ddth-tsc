package com.github.ddth.tsc.mem;

import java.util.Arrays;

import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.DataPoint;
import com.google.common.util.concurrent.AtomicLongMap;

/**
 * //TODO
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

    private Long calcKey(long timestamp) {
        long delta = timestamp % RESOLUTION_MS;
        return Long.valueOf(timestamp - delta);
    }

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
    public void add(long timestamp, long value) {
        Long key = calcKey(timestamp);
        counter.addAndGet(key, value);
        if (counter.size() > maxNumBlocks) {
            reduce();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint get(long timestampMs) {
        Long key = calcKey(timestampMs);
        long value = counter.get(key);
        return new DataPoint(key.longValue(), value, RESOLUTION_MS);
    }
}
