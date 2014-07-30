package com.github.ddth.tsc;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import com.github.ddth.tsc.DataPoint.Type;
import com.google.common.primitives.Longs;

/**
 * Abstract implementation of {@link ICounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class AbstractCounter implements ICounter {

    private AbstractCounterFactory counterFactory;
    private String name;

    public AbstractCounter() {
    }

    public AbstractCounter(String name) {
        setName(name);
    }

    /**
     * Gets the associated counter factory.
     * 
     * @return
     * @since 0.4.2
     */
    public AbstractCounterFactory getCounterFactory() {
        return counterFactory;
    }

    /**
     * Associates with a counter factory.
     * 
     * @param counterFactory
     * @return
     * @since 0.4.2
     */
    public AbstractCounter setCounterFactory(AbstractCounterFactory counterFactory) {
        this.counterFactory = counterFactory;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
     * Converts a timestamp to time series point.
     * 
     * @param timestampMs
     * @param steps
     * @return
     * @since 0.3.0
     */
    protected Long toTimeSeriesPoint(long timestampMs, int steps) {
        long delta = timestampMs % (RESOLUTION_MS * steps);
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
    public abstract void add(long timestampMs, long value);

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(long value) {
        set(System.currentTimeMillis(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void set(long timestampMs, long value);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract DataPoint get(long timestampMs);

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
            DataPoint _temp = get(_key);
            result.add(_temp);
            _key += RESOLUTION_MS;
        }
        return result;
    }

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
    public DataPoint[] getSeries(long timestampStartMs, DataPoint.Type type) {
        return getSeries(timestampStartMs, 1, type);
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
    @Override
    public DataPoint[] getSeries(long timestampStartMs, int steps, DataPoint.Type type) {
        return getSeries(timestampStartMs, System.currentTimeMillis(), steps, type);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs) {
        return getSeries(timestampStartMs, timestampEndMs, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs, DataPoint.Type type) {
        return getSeries(timestampStartMs, timestampEndMs, 1, type);
    }

    /**
     * Gets all data points in range [{@code timestampStartMs},
     * {@code timestampEndMs}).
     * 
     * @param timestampStartMs
     * @param timestampEndMs
     * @return
     * @since 0.3.2
     */
    protected DataPoint[] getAllInRange(long timestampStartMs, long timestampEndMs) {
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
        return result.toArray(DataPoint.EMPTY_ARR);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs, int steps) {
        return getSeries(timestampStartMs, timestampEndMs, steps, DataPoint.Type.SUM);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs, int steps,
            DataPoint.Type type) {
        DataPoint[] origin = getAllInRange(timestampStartMs, timestampEndMs);

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
                block = new DataPoint(DataPoint.Type.NONE, t, 0, blockSize);
                result[resultIndex] = block;
            }
            if (block.type() == DataPoint.Type.NONE && org.type() != DataPoint.Type.NONE) {
                block.type(type);
            }
            block.add(org);

            orgIndex++;
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getLastN(int n) {
        return getLastN(n, 1, Type.SUM);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getLastN(int n, DataPoint.Type type) {
        return getLastN(n, 1, type);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getLastN(int n, int steps) {
        return getLastN(n, steps, Type.SUM);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint[] getLastN(int n, int steps, DataPoint.Type type) {
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
        return getSeries(timestampStart, currentTimestamp + 1, steps, type);
    }
}
