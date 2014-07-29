package com.github.ddth.tsc.cassandra;

import java.text.MessageFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.tsc.AbstractCounter;
import com.github.ddth.tsc.DataPoint;
import com.github.ddth.tsc.DataPoint.Type;
import com.github.ddth.tsc.cassandra.internal.CassandraUtils;
import com.github.ddth.tsc.cassandra.internal.CounterMetadata;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

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
        if (!metadata.isCounterColumn) {
            throw new IllegalStateException("Counter [" + getName()
                    + "] does not support ADD operator!");
        }
        Long key = toTimeSeriesPoint(timestampMs);
        int[] yyyymm_dd = toYYYYMM_DD(timestampMs);
        CassandraUtils.executeNonSelect(session, pStmAdd, value, getName(), yyyymm_dd[0],
                yyyymm_dd[1], key.longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(long timestampMs, long value) {
        if (metadata.isCounterColumn) {
            throw new IllegalStateException("Counter [" + getName()
                    + "] does not support SET operator!");
        }
        Long key = toTimeSeriesPoint(timestampMs);
        int[] yyyymm_dd = toYYYYMM_DD(timestampMs);
        CassandraUtils.executeNonSelect(session, pStmSet, value, getName(), yyyymm_dd[0],
                yyyymm_dd[1], key.longValue());
    }

    /**
     * Gets all data points of a day (used by cache loader).
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

    private ThreadLocal<LoadingCache<Integer, Map<Long, DataPoint>>> cacheRow = new ThreadLocal<LoadingCache<Integer, Map<Long, DataPoint>>>() {
        @Override
        protected LoadingCache<Integer, Map<Long, DataPoint>> initialValue() {
            CacheLoader<Integer, Map<Long, DataPoint>> loaderRow = new CacheLoader<Integer, Map<Long, DataPoint>>() {
                @Override
                public Map<Long, DataPoint> load(Integer yyyymmdd) throws Exception {
                    int yyyymm = yyyymmdd / 100;
                    int dd = yyyymmdd % 100;
                    return _getRow(getName(), yyyymm, dd);
                }
            };
            return CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.SECONDS)
                    .build(loaderRow);
        }
    };

    /**
     * Gets all data points of a day specified by the timestamp, cache
     * supported.
     * 
     * @param timestampMs
     * @return
     */
    private Map<Long, DataPoint> _getRowWithCache(long timestampMs) {
        int[] yyyymm_dd = toYYYYMM_DD(timestampMs);
        int yyyymmdd = yyyymm_dd[0] * 100 + yyyymm_dd[1];
        try {
            return cacheRow.get().get(yyyymmdd);
        } catch (ExecutionException e) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataPoint[] getAllInRange(long timestampStartMs, long timestampEndMs) {
        try {
            // initialize cache
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(timestampStartMs);
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            while (cal.getTimeInMillis() <= timestampEndMs) {
                int yyyy = cal.get(Calendar.YEAR);
                int mm = cal.get(Calendar.MONTH) + 1;
                int dd = cal.get(Calendar.DATE);
                int yyyymmdd = yyyy * 10000 + mm * 100 + dd;
                try {
                    cacheRow.get().get(yyyymmdd);
                } catch (ExecutionException e) {
                }
                cal.add(Calendar.DATE, 1);
            }
            return super.getAllInRange(timestampStartMs, timestampEndMs);
        } finally {
            cacheRow.remove();
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
