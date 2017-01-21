package com.github.ddth.tsc.qnd;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ddth.cql.SessionManager;
import com.github.ddth.tsc.ICounter;
import com.github.ddth.tsc.cassandra.CassandraCounterFactory;

public class QndPerformanceTest {

    public static void main(String[] args) throws InterruptedException {
        final SessionManager sessionManager = new SessionManager();
        sessionManager.init();

        final CassandraCounterFactory counterFactory = new CassandraCounterFactory()
                .setHostsAndPorts("localhost:9042").setKeyspace("stats_bcb_dev")
                .setUsername("stats_bcb").setPassword("stats_bcb").setTableMetadata("tsc_metadata")
                .setSessionManager(sessionManager).init();
        final ICounter counter1 = counterFactory.getCounter("counter_1");
        // counter1.add(1);

        final int NUM_LOOPS = 20000;
        final int NUM_THREADS = 16;
        final AtomicLong COUNTER = new AtomicLong();
        final CountDownLatch countDownLatach = new CountDownLatch(NUM_THREADS);
        final long timestampStart = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            Thread t = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_LOOPS; i++) {
                        counter1.add(1);
                        COUNTER.incrementAndGet();
                    }
                    countDownLatach.countDown();
                }
            };
            t.start();
        }

        countDownLatach.await();
        final long timestampEnd = System.currentTimeMillis();

        counterFactory.destroy();
        sessionManager.destroy();

        long duration = timestampEnd - timestampStart;
        long counter = COUNTER.get();
        System.out.println("[" + counter + "] writes in [" + duration + " ms].");
        System.out.println(counter * 1000.0 / duration);
    }
}
