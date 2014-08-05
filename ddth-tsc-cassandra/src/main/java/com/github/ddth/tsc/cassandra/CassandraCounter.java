package com.github.ddth.tsc.cassandra;

import java.text.MessageFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.cacheadapter.ICache;
import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;
import com.github.ddth.tsc.cassandra.internal.CassandraUtils;
import com.github.ddth.tsc.cassandra.internal.CounterMetadata;

/**
 * Cassandra-backed counter.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.2.0
 */
public class CassandraCounter extends AbstractCounter {

    private Session session;
    private PreparedStatement pStmAdd, pStmSet, pStmGet, pStmGetRow;
    private CounterMetadata metadata;
    private ICacheFactory cacheFactory;

    public CassandraCounter() {
    }

    public CassandraCounter(String name, Session session, CounterMetadata metadata) {
        super(name);
        setSession(session).setMetadata(metadata);
    }

    protected Session getSession() {
        return session;
    }

    public CassandraCounter setSession(Session session) {
        this.session = session;
        return this;
    }

    protected CounterMetadata getMetadata() {
        return metadata;
    }

    public CassandraCounter setMetadata(CounterMetadata metadata) {
        this.metadata = metadata;
        return this;
    }

    public CassandraCounter setCacheFactory(ICacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
        return this;
    }

    protected ICacheFactory getCacheFactory() {
        return cacheFactory;
    }

    private void _initPreparedStatements() {
        String tableName = metadata.table;

        if (metadata.isCounterColumn) {
            String cqlAdd = MessageFormat.format(CqlTemplate.CQL_TEMPLATE_ADD_COUNTER, tableName);
            pStmAdd = session.prepare(cqlAdd);
        } else {
            String cqlSet = MessageFormat.format(CqlTemplate.CQL_TEMPLATE_SET_COUNTER, tableName);
            pStmSet = session.prepare(cqlSet);
        }

        String cqlGet = MessageFormat.format(CqlTemplate.CQL_TEMPLATE_GET_COUNTER, tableName);
        pStmGet = session.prepare(cqlGet);

        String cqlGetRow = MessageFormat
                .format(CqlTemplate.CQL_TEMPLATE_GET_COUNTER_ROW, tableName);
        pStmGetRow = session.prepare(cqlGetRow);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init() {
        super.init();

        _initPreparedStatements();
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
            throw new IllegalArgumentException("Argument must be an instance of "
                    + CassandraCounterFactory.class.getName());
        }
        return this;
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
        Long key = toTimeSeriesPoint(timestampMs);
        int[] yyyymm_dd = toYYYYMM_DD(timestampMs);

        if (!metadata.isCounterColumn) {
            Row row = CassandraUtils.executeOne(session, pStmGet, getName(), yyyymm_dd[0],
                    yyyymm_dd[1], key.longValue());
            long currentValue = row != null ? row.getLong("v") : 0;
            long newValue = value + currentValue;
            set(timestampMs, newValue);
        } else {
            CassandraUtils.executeNonSelect(session, pStmAdd, value, getName(), yyyymm_dd[0],
                    yyyymm_dd[1], key.longValue());
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
        Long key = toTimeSeriesPoint(timestampMs);
        int[] yyyymm_dd = toYYYYMM_DD(timestampMs);

        if (metadata.isCounterColumn) {
            Row row = CassandraUtils.executeOne(session, pStmGet, getName(), yyyymm_dd[0],
                    yyyymm_dd[1], key.longValue());
            long currentValue = row != null ? row.getLong("v") : 0;
            long delta = value - currentValue;
            add(timestampMs, delta);
        } else {
            CassandraUtils.executeNonSelect(session, pStmSet, value, getName(), yyyymm_dd[0],
                    yyyymm_dd[1], key.longValue());
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
        Long _key = toTimeSeriesPoint(timestampMs);
        Map<Long, DataPoint> row = _getRowWithCache(timestampMs);
        DataPoint result = row != null ? row.get(_key) : null;
        return result != null ? result : new DataPoint(Type.NONE, _key.longValue(), 0,
                RESOLUTION_MS);
    }

    private ICache getCache() {
        return cacheFactory != null ? cacheFactory.createCache(getName()) : null;
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

        ResultSet rs = CassandraUtils.execute(session, pStmGetRow, counterName, yyyymm, dd);
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
