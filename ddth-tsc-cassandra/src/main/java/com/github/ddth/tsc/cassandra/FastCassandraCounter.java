package com.github.ddth.tsc.cassandra;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.datastax.driver.core.Session;
import com.github.ddth.tsc.cassandra.internal.CassandraUtils;
import com.github.ddth.tsc.cassandra.internal.CounterMetadata;

/**
 * Cassandra-backed counter.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.2.0
 */
public class FastCassandraCounter extends CassandraCounter {

    private int queueSize = 100;
    private int tickDuration = 1000;
    private ScheduledExecutorService executorService;
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Queue<UpdateEntry> queue = new ConcurrentLinkedQueue<UpdateEntry>();

    public FastCassandraCounter() {
    }

    public FastCassandraCounter(String name, Session session, CounterMetadata metadata) {
        super(name, session, metadata);
    }

    protected int getQueueSize() {
        return queueSize;
    }

    public FastCassandraCounter setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    protected int getTickDuration() {
        return tickDuration;
    }

    public FastCassandraCounter setTickDuration(int tickDuration) {
        this.tickDuration = tickDuration;
        return this;
    }

    protected ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    public FastCassandraCounter setExecutorService(ScheduledExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    private void flush() {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            if (queue.size() > 0) {
                CounterMetadata metadata = getMetadata();
                String tableName = metadata.table;
                String counterName = getName();

                StringBuilder CQL = new StringBuilder(
                        metadata.isCounterColumn ? "BEGIN COUNTER BATCH" : "BEGIN BATCH")
                        .append("\n");
                UpdateEntry updateEntry = queue.poll();
                while (updateEntry != null) {
                    CQL.append("\t");
                    Long key = toTimeSeriesPoint(updateEntry.timestamp);
                    int[] yyyymm_dd = toYYYYMM_DD(updateEntry.timestamp);
                    if (updateEntry.type == UpdateEntry.Type.ADD) {
                        CQL.append("UPDATE ").append(tableName).append(" SET v=v+")
                                .append(updateEntry.value).append(" WHERE c='").append(counterName)
                                .append("' AND ym=").append(yyyymm_dd[0]).append(" AND d=")
                                .append(yyyymm_dd[1]).append(" AND t=").append(key.longValue());
                    } else {
                        CQL.append("UPDATE ").append(tableName).append(" SET v=")
                                .append(updateEntry.value).append(" WHERE c='").append(counterName)
                                .append("' AND ym=").append(yyyymm_dd[0]).append(" AND d=")
                                .append(yyyymm_dd[1]).append(" AND t=").append(key.longValue());
                    }
                    CQL.append("\n");
                    updateEntry = queue.poll();
                }
                CQL.append("APPLY BATCH;");
                CassandraUtils.executeNonSelect(getSession(), CQL.toString());
                // System.out.println(CQL.toString());
            }
        } finally {
            lock.unlock();
        }
    }

    private class ScheduledUpdator implements Runnable {
        @Override
        public void run() {
            flush();
        }
    }

    private static class UpdateEntry {
        private static enum Type {
            ADD, SET
        };

        public Type type = Type.ADD;
        public long timestamp, value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init() {
        super.init();

        ScheduledUpdator updator = new ScheduledUpdator();
        executorService.scheduleWithFixedDelay(updator, tickDuration, tickDuration,
                TimeUnit.MILLISECONDS);
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
     * {@inheritDoc}
     */
    @Override
    public void add(long timestampMs, long value) {
        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            UpdateEntry entry = new UpdateEntry();
            entry.type = UpdateEntry.Type.ADD;
            entry.timestamp = timestampMs;
            entry.value = value;
            queue.add(entry);
        } finally {
            lock.unlock();
        }
        if (queue.size() > queueSize) {
            flush();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(long timestampMs, long value) {
        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            UpdateEntry entry = new UpdateEntry();
            entry.type = UpdateEntry.Type.SET;
            entry.timestamp = timestampMs;
            entry.value = value;
            queue.add(entry);
        } finally {
            lock.unlock();
        }
        if (queue.size() > queueSize) {
            flush();
        }
    }
}
