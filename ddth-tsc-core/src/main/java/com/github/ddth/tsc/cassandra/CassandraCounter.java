package com.github.ddth.tsc.cassandra;

import java.text.MessageFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.github.ddth.cacheadapter.ICache;
import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.cacheadapter.guava.GuavaCacheFactory;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;
import com.github.ddth.tsc.cassandra.internal.CounterMetadata;
import com.github.ddth.tsc.cassandra.internal.CqlTemplate;
import com.github.ddth.tsc.cassandra.internal.SessionHelper;

/**
 * Cassandra-backed counter.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.2.0
 */
public class CassandraCounter extends AbstractCounter {

    private CounterMetadata metadata;
    private SessionHelper sessionHelper;

    private String cqlAdd, cqlSet, cqlGet, cqlGetRow;

    public CassandraCounter() {
    }

    public CassandraCounter(SessionHelper sessionHelper, String name, CounterMetadata metadata) {
        super(name);
        setMetadata(metadata);
        setSessionHelper(sessionHelper);
    }

    public CounterMetadata getMetadata() {
        return metadata;
    }

    public CassandraCounter setMetadata(CounterMetadata metadata) {
        this.metadata = metadata;
        return this;
    }

    /**
     * @return
     * @since 0.7.0
     */
    public SessionHelper getSessionHelper() {
        return sessionHelper;
    }

    /**
     * @param sessionHelper
     * @return
     * @since 0.7.0
     */
    public CassandraCounter setSessionHelper(SessionHelper sessionHelper) {
        this.sessionHelper = sessionHelper;
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @since 0.4.2
     */
    @Override
    public CassandraCounterFactory getCounterFactory() {
        return (CassandraCounterFactory) super.getCounterFactory();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CassandraCounter setCounterFactory(AbstractCounterFactory counterFactory) {
        if (counterFactory instanceof CassandraCounterFactory) {
            super.setCounterFactory(counterFactory);
        } else {
            throw new IllegalArgumentException(
                    "Argument must be an instance of " + CassandraCounterFactory.class.getName());
        }
        return this;
    }

    private void _initStatements() {
        String tableName = metadata.getTable();
        cqlAdd = MessageFormat.format(CqlTemplate.CQL_TEMPLATE_ADD_COUNTER, tableName);
        cqlSet = MessageFormat.format(CqlTemplate.CQL_TEMPLATE_SET_COUNTER, tableName);
        cqlGet = MessageFormat.format(CqlTemplate.CQL_TEMPLATE_GET_COUNTER, tableName);
        cqlGetRow = MessageFormat.format(CqlTemplate.CQL_TEMPLATE_GET_COUNTER_ROW, tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init() {
        super.init();
        _initStatements();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        super.destroy();
    }

    /*----------------------------------------------------------------------*/
    /**
     * @since 0.5.1
     */
    protected ThreadLocal<ICacheFactory> threadLocalCache = new ThreadLocal<ICacheFactory>() {
        @SuppressWarnings("resource")
        @Override
        protected ICacheFactory initialValue() {
            return new GuavaCacheFactory().setDefaultCacheCapacity(10000)
                    .setDefaultExpireAfterAccess(5 * 60).init();
        }
    };

    /**
     * @since 0.4.2
     */
    private ICache getCache() {
        String cacheName = getName();
        ICache cache = sessionHelper.getCache(cacheName);
        return cache != null ? cache : threadLocalCache.get().createCache(cacheName);
    }

    private ConsistencyLevel getConsistencyLevelForRead() {
        return getCounterFactory().getConsistencyLevelForRead();
    }

    private ConsistencyLevel getConsistencyLevelForWrite() {
        return getCounterFactory().getConsistencyLevelForWrite();
    }

    private ConsistencyLevel getConsistencyLevelForReadForUpdate() {
        return getCounterFactory().getConsistencyLevelForReadForUpdate();
    }

    /*----------------------------------------------------------------------*/

    protected static int[] toYYYYMM_DD(long timestampMs) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestampMs);

        int yyyy = cal.get(Calendar.YEAR);
        int mm = cal.get(Calendar.MONTH) + 1;
        int dd = cal.get(Calendar.DAY_OF_MONTH);

        return new int[] { yyyy * 100 + mm, dd };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(long timestampMs, long value) {
        long key = toTimeSeriesPoint(timestampMs);
        int[] yyyymm_dd = toYYYYMM_DD(timestampMs);

        if (!metadata.isCounterColumn()) {
            Row row = CqlUtils.executeOne(sessionHelper.getSession(), cqlGet,
                    getConsistencyLevelForReadForUpdate(), getName(), yyyymm_dd[0], yyyymm_dd[1],
                    key);
            long currentValue = row != null ? row.getLong("v") : 0;
            long newValue = value + currentValue;
            set(timestampMs, newValue);
        } else {
            CqlUtils.executeNonSelectAsync(sessionHelper.getSession(), cqlAdd,
                    getConsistencyLevelForWrite(), value, getName(), yyyymm_dd[0], yyyymm_dd[1],
                    key);
        }
        ICache cache = getCache();
        if (cache != null) {
            cache.delete(String.valueOf(yyyymm_dd[0] * 100 + yyyymm_dd[1]));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(long timestampMs, long value) {
        long key = toTimeSeriesPoint(timestampMs);
        int[] yyyymm_dd = toYYYYMM_DD(timestampMs);

        if (metadata.isCounterColumn()) {
            Row row = CqlUtils.executeOne(sessionHelper.getSession(), cqlGet,
                    getConsistencyLevelForReadForUpdate(), getName(), yyyymm_dd[0], yyyymm_dd[1],
                    key);
            long currentValue = row != null ? row.getLong("v") : 0;
            long delta = value - currentValue;
            add(timestampMs, delta);
        } else {
            CqlUtils.executeNonSelectAsync(sessionHelper.getSession(), cqlSet,
                    getConsistencyLevelForWrite(), value, getName(), yyyymm_dd[0], yyyymm_dd[1],
                    key);
        }
        ICache cache = getCache();
        if (cache != null) {
            cache.delete(String.valueOf(yyyymm_dd[0] * 100 + yyyymm_dd[1]));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataPoint get(long timestampMs) {
        long _key = toTimeSeriesPoint(timestampMs);
        Map<Long, DataPoint> row = _getRowWithCache(timestampMs);
        DataPoint result = row != null ? row.get(_key) : null;
        return result != null ? result : new DataPoint(Type.NONE, _key, 0, RESOLUTION_MS);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.5.1
     */
    protected DataPoint[] getAllInRange(long timestampStartMs, long timestampEndMs) {
        try {
            return super.getAllInRange(timestampStartMs, timestampEndMs);
        } finally {
            threadLocalCache.remove();
        }
    }

    /**
     * Gets all data points of a day specified by the timestamp, cache
     * supported.
     * 
     * @param timestampMs
     * @return
     */
    @SuppressWarnings("unchecked")
    private Map<Long, DataPoint> _getRowWithCache(long timestampMs) {
        int[] yyyymm_dd = toYYYYMM_DD(timestampMs);
        int yyyymmdd = yyyymm_dd[0] * 100 + yyyymm_dd[1];
        ICache cache = getCache();
        String cacheKey = String.valueOf(yyyymmdd);
        Object temp = cache != null ? cache.get(cacheKey) : null;
        Map<Long, DataPoint> result = (Map<Long, DataPoint>) (temp instanceof Map ? temp : null);
        if (result == null) {
            result = _getRow(getName(), yyyymm_dd[0], yyyymm_dd[1]);
            if (cache != null) {
                cache.set(cacheKey, result);
            }
        }
        return result;
    }

    /**
     * Gets all data points of a day.
     * 
     * @param counterName
     * @param yyyymm
     * @param dd
     * @return
     * @since 0.3.1.1
     */
    private Map<Long, DataPoint> _getRow(String counterName, int yyyymm, int dd) {
        Map<Long, DataPoint> result = new HashMap<Long, DataPoint>();

        ResultSet rs = CqlUtils.execute(sessionHelper.getSession(), cqlGetRow,
                getConsistencyLevelForRead(), counterName, yyyymm, dd);
        for (Iterator<Row> it = rs.iterator(); it.hasNext();) {
            Row row = it.next();
            long key = row.getLong(CqlTemplate.COL_COUNTER_TIMESTAMP);
            long value = row.getLong(CqlTemplate.COL_COUNTER_VALUE);
            DataPoint dp = new DataPoint(Type.SUM, key, value, RESOLUTION_MS);
            result.put(key, dp);
        }

        return result;
    }
}
