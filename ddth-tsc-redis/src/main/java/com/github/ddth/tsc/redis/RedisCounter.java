package com.github.ddth.tsc.redis;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;

import com.github.ddth.redis.IRedisClient;
import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;
import com.google.common.primitives.Longs;

/**
 * Redis-backed counter.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class RedisCounter extends AbstractCounter {

    private int ttlSeconds = RedisCounterFactory.DEFAULT_TTL_SECONDS;
    private long BUCKET_SIZE = 60;

    public RedisCounter() {
    }

    public RedisCounter(String name, int ttlSeconds) {
        super(name);
        setTTL(ttlSeconds);
    }

    public int getTTL() {
        return ttlSeconds;
    }

    public RedisCounter setTTL(int ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init() {
        super.init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        super.destroy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RedisCounterFactory getCounterFactory() {
        return (RedisCounterFactory) super.getCounterFactory();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RedisCounter setCounterFactory(AbstractCounterFactory counterFactory) {
        if (counterFactory instanceof RedisCounterFactory) {
            super.setCounterFactory(counterFactory);
        } else {
            throw new IllegalArgumentException("Argument must be an instance of "
                    + RedisCounterFactory.class.getName());
        }
        return this;
    }

    /*----------------------------------------------------------------------*/

    private long[] calcBucketOffset(long timestampMs) {
        long bucketOffset = toTimeSeriesPoint(timestampMs);
        long delta = bucketOffset % (RESOLUTION_MS * BUCKET_SIZE);
        long bucketId = bucketOffset - delta;
        return new long[] { bucketId, bucketOffset };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(long timestampMs, long value) {
        long[] bucket = calcBucketOffset(timestampMs);
        String redisKey = getName() + ":" + bucket[0];
        String redisField = String.valueOf(bucket[1]);
        IRedisClient redisClient = getCounterFactory().getRedisClient();
        try {
            redisClient.hashIncBy(redisKey, redisField, value);
            redisClient.expire(redisKey, ttlSeconds);
        } finally {
            redisClient.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(long timestampMs, long value) {
        long[] bucket = calcBucketOffset(timestampMs);
        String redisKey = getName() + ":" + bucket[0];
        String redisField = String.valueOf(bucket[1]);
        IRedisClient redisClient = getCounterFactory().getRedisClient();
        try {
            redisClient.hashSet(redisKey, redisField, String.valueOf(value), ttlSeconds);
        } finally {
            redisClient.close();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.4.1
     */
    @Override
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

        // build list of Redis map & field names
        List<Long> keys = new ArrayList<Long>();
        List<String> mapNames = new ArrayList<String>();
        List<String> fieldNames = new ArrayList<String>();
        String _name = getName();
        for (long timestamp = keyStart.longValue(), _end = keyEnd.longValue(); timestamp <= _end; timestamp += RESOLUTION_MS) {
            long bucketOffset = toTimeSeriesPoint(timestamp);
            long delta = bucketOffset % (RESOLUTION_MS * BUCKET_SIZE);
            long bucketId = bucketOffset - delta;
            long[] bucket = { bucketId, bucketOffset };
            keys.add(bucketOffset);
            String redisKey = _name + ":" + bucket[0];
            String redisField = String.valueOf(bucket[1]);
            mapNames.add(redisKey);
            fieldNames.add(redisField);
        }

        // use pipeline to get all datapoint values at once
        IRedisClient redisClient = getCounterFactory().getRedisClient();
        List<String> _pointValues;
        try {
            _pointValues = redisClient.hashMultiGet(
                    mapNames.toArray(ArrayUtils.EMPTY_STRING_ARRAY),
                    fieldNames.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
        } finally {
            redisClient.close();
        }

        for (int i = 0, n = keys.size(); i < n; i++) {
            Long _key = keys.get(i);
            Long _value = null;
            try {
                _value = Long.parseLong(_pointValues.get(i));
            } catch (Exception e) {
                _value = null;
            }
            DataPoint dp = _value != null ? new DataPoint(Type.SUM, _key.longValue(),
                    _value.longValue(), RESOLUTION_MS) : new DataPoint(Type.NONE, _key.longValue(),
                    0, RESOLUTION_MS);
            result.add(dp);
        }
        return result.toArray(DataPoint.EMPTY_ARR);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint get(long timestampMs) {
        long[] bucket = calcBucketOffset(timestampMs);
        String redisKey = getName() + ":" + bucket[0];
        String redisField = String.valueOf(bucket[1]);
        IRedisClient redisClient = getCounterFactory().getRedisClient();
        try {
            Long result = null;
            try {
                result = Long.parseLong(redisClient.hashGet(redisKey, redisField));
            } catch (Exception e) {
                result = null;
            }
            Long _key = toTimeSeriesPoint(timestampMs);
            return result != null ? new DataPoint(Type.SUM, _key.longValue(), result.longValue(),
                    RESOLUTION_MS) : new DataPoint(Type.NONE, _key.longValue(), 0, RESOLUTION_MS);
        } finally {
            redisClient.close();
        }
    }
}
