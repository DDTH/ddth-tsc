package com.github.ddth.tsc;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.primitives.Longs;

/**
 * Abstract implementation of {@link ICounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class AbstractCounter implements ICounter {

    protected final static int RESOLUTION_MS = 1000; // 1 sec
    private String name;

    public AbstractCounter() {
    }

    public AbstractCounter(String name) {
        setName(name);
    }

    public String getName() {
        return name;
    }

    public AbstractCounter setName(String name) {
        this.name = name;
        return this;
    }

    public void init() {
    }

    public void destroy() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(long value) {
        add(System.currentTimeMillis(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void add(long timestamp, long value);

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getSeries(long timestampStartMs) {
        return getSeries(timestampStartMs, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getSeries(long timestampStartMs, int steps) {
        return getSeries(timestampStartMs, System.currentTimeMillis(), steps);
    }

    /**
     * {@inheritDoc}
     */
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs) {
        return getSeries(timestampStartMs, timestampEndMs, 1);
    }

    /*
     * //TODO review performance?
     */
    private DataPoint[] _get(long timestampStartMs, long timestampEndMs) {
        SortedSet<DataPoint> result = new TreeSet<DataPoint>(new Comparator<DataPoint>() {
            @Override
            public int compare(DataPoint block1, DataPoint block2) {
                return Longs.compare(block1.timestamp(), block2.timestamp());
            }
        });
        for (long timestamp = timestampStartMs; timestamp < timestampEndMs; timestamp++) {
            DataPoint block = get(timestamp);
            result.add(block);
        }
        return result.toArray(DataPoint.EMPTY_COUNTER_BLOCK_ARR);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs, int steps) {
        DataPoint[] origin = _get(timestampStartMs, timestampEndMs);

        if (steps < 1) {
            steps = 1;
        }
        if (steps == 1) {
            return origin;
        }

        int n = origin.length / steps;
        if (n * steps < origin.length) {
            n++;
        }
        DataPoint[] result = new DataPoint[n];

        int orgIndex = 0;
        int blockSize = steps * RESOLUTION_MS;
        for (DataPoint org : origin) {
            int resultIndex = orgIndex / steps;
            DataPoint block = result[resultIndex];
            if (block == null) {
                long t = org.timestamp();
                long delta = t % blockSize;
                t -= delta;
                block = new DataPoint(t, 0, blockSize);
                result[resultIndex] = block;
            }
            block.add(org.value());

            orgIndex++;
        }

        return result;
    }
}
