package com.github.ddth.tsc.redis;

import com.github.ddth.redis.IRedisClient;
import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;

/**
 * Redis-backed counter.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class RedisCounter extends AbstractCounter {

    private RedisCounterFactory counterFactory;
    private int ttlSeconds = RedisCounterFactory.DEFAULT_TTL_SECONDS;
    private long BUCKET_SIZE = 60;

    public RedisCounter() {
    }

    public RedisCounter(String name, RedisCounterFactory counterFactory, int ttlSeconds) {
        super(name);
        setCounterFactory(counterFactory);
        setTTL(ttlSeconds);
    }

    public RedisCounterFactory getCounterFactory() {
        return counterFactory;
    }

    public RedisCounter setCounterFactory(RedisCounterFactory counterFactory) {
        this.counterFactory = counterFactory;
        return this;
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
        IRedisClient redisClient = counterFactory.getRedisClient();
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
        IRedisClient redisClient = counterFactory.getRedisClient();
        try {
            redisClient.hashSet(redisKey, redisField, String.valueOf(value), ttlSeconds);
        } finally {
            redisClient.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint get(long timestampMs) {
        long[] bucket = calcBucketOffset(timestampMs);
        String redisKey = getName() + ":" + bucket[0];
        String redisField = String.valueOf(bucket[1]);
        IRedisClient redisClient = counterFactory.getRedisClient();
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
}
