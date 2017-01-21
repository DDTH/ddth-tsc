package com.github.ddth.tsc.redis;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;
import com.google.common.primitives.Longs;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

/**
 * ShardedRedis-backed counter.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class ShardedRedisCounter extends AbstractCounter {

    private int ttlSeconds = ShardedRedisCounterFactory.DEFAULT_TTL_SECONDS;
    private long BUCKET_SIZE = 60;

    public ShardedRedisCounter() {
    }

    public ShardedRedisCounter(String name, int ttlSeconds) {
        super(name);
        setTtl(ttlSeconds);
    }

    public int getTtl() {
        return ttlSeconds;
    }

    public ShardedRedisCounter setTtl(int ttlSeconds) {
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
    public ShardedRedisCounterFactory getCounterFactory() {
        return (ShardedRedisCounterFactory) super.getCounterFactory();
    }

    private ShardedJedis getJedis() {
        return getCounterFactory().getJedis();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ShardedRedisCounter setCounterFactory(AbstractCounterFactory counterFactory) {
        if (counterFactory instanceof ShardedRedisCounterFactory) {
            super.setCounterFactory(counterFactory);
        } else {
            throw new IllegalArgumentException("Argument must be an instance of "
                    + ShardedRedisCounterFactory.class.getName());
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
        try (ShardedJedis jedis = getJedis()) {
            jedis.hincrBy(redisKey, redisField, value);
            if (ttlSeconds > 0) {
                jedis.expire(redisKey, ttlSeconds);
            }
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
        try (ShardedJedis jedis = getJedis()) {
            jedis.hset(redisKey, redisField, String.valueOf(value));
            if (ttlSeconds > 0) {
                jedis.expire(redisKey, ttlSeconds);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataPoint[] getAllInRange(long timestampStartMs, long timestampEndMs) {
        SortedSet<DataPoint> result = new TreeSet<DataPoint>(new Comparator<DataPoint>() {
            @Override
            public int compare(DataPoint block1, DataPoint block2) {
                return Longs.compare(block1.timestamp(), block2.timestamp());
            }
        });
        long keyStart = toTimeSeriesPoint(timestampStartMs);
        long keyEnd = toTimeSeriesPoint(timestampEndMs);
        if (keyEnd == timestampStartMs) {
            keyEnd = toTimeSeriesPoint(timestampEndMs - 1);
        }

        // build list of Redis map & field names
        List<Long> keys = new ArrayList<Long>();
        List<String> mapNames = new ArrayList<String>();
        List<String> fieldNames = new ArrayList<String>();
        String _name = getName();
        for (long timestamp = keyStart, _end = keyEnd; timestamp <= _end; timestamp += RESOLUTION_MS) {
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

        // use pipeline to get all data points at once
        try (ShardedJedis jedis = getJedis()) {
            ShardedJedisPipeline p = jedis.pipelined();
            for (int i = 0, n = mapNames.size(); i < n; i++) {
                String mapName = mapNames.get(i);
                String fieldName = fieldNames.get(i);
                p.hget(mapName, fieldName);
            }
            List<?> _pointValues = p.syncAndReturnAll();
            for (int i = 0, n = keys.size(); i < n; i++) {
                Long _key = keys.get(i);
                Long _value = null;
                try {
                    _value = Long.parseLong(_pointValues.get(i).toString());
                } catch (Exception e) {
                    _value = null;
                }
                DataPoint dp = _value != null
                        ? new DataPoint(Type.SUM, _key.longValue(), _value.longValue(),
                                RESOLUTION_MS)
                        : new DataPoint(Type.NONE, _key.longValue(), 0, RESOLUTION_MS);
                result.add(dp);
            }
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
        try (ShardedJedis jedis = getJedis()) {
            Long result = null;
            try {
                result = Long.parseLong(jedis.hget(redisKey, redisField));
            } catch (Exception e) {
                result = null;
            }
            long _key = toTimeSeriesPoint(timestampMs);
            return result != null ? new DataPoint(Type.SUM, _key, result.longValue(), RESOLUTION_MS)
                    : new DataPoint(Type.NONE, _key, 0, RESOLUTION_MS);
        }
    }
}
