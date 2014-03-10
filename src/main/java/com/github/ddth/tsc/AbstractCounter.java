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

    /**
     * Initializing method.
     */
    public void init() {
        // EMPTY
    }

    /**
     * Destroying method.
     */
    public void destroy() {
        // EMPTY
    }

    /**
     * Converts a timestamp to time series point.
     * 
     * @param timestampMs
     * @return
     * @since 0.2.0
     */
    protected Long toTimeSeriesPoint(long timestampMs) {
        long delta = timestampMs % RESOLUTION_MS;
        return Long.valueOf(timestampMs - delta);
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

    /**
     * Gets all data points in range [{@code timestampStartMs},
     * {@code timestampEndMs}).
     * 
     * @param timestampStartMs
     * @param timestampEndMs
     * @return
     */
    private DataPoint[] _get(long timestampStartMs, long timestampEndMs) {
        SortedSet<DataPoint> result = new TreeSet<DataPoint>(new Comparator<DataPoint>() {
            @Override
            public int compare(DataPoint block1, DataPoint block2) {
                return Longs.compare(block1.timestamp(), block2.timestamp());
            }
        });
        Long keyStart = toTimeSeriesPoint(timestampStartMs);
        Long keyEnd = toTimeSeriesPoint(timestampEndMs);
        if (keyEnd.longValue() == timestampStartMs) {
            keyEnd = toTimeSeriesPoint(timestampEndMs - 1);
        }
        for (long timestamp = keyStart.longValue(), _end = keyEnd.longValue(); timestamp <= _end; timestamp += RESOLUTION_MS) {
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

    /**
     * {@inheritDoc}
     */
    public DataPoint[] getLastN(int n) {
        return getLastN(n, 1);
    }

    public DataPoint[] getLastN(int n, int steps) {
        if (steps < 1) {
            steps = 1;
        }
        if (n < 1) {
            n = 1;
        }
        long currentTimestamp = System.currentTimeMillis();
        int blockSize = RESOLUTION_MS * steps;
        long delta = currentTimestamp % blockSize;
        long timestampStart = currentTimestamp - delta - (n - 1) * blockSize;
        return getSeries(timestampStart, currentTimestamp + 1, steps);
    }
}
