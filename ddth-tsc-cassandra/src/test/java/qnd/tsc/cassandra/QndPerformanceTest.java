package qnd.tsc.cassandra;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ddth.tsc.ICounter;
import com.github.ddth.tsc.cassandra.CassandraCounterFactory;

public class QndPerformanceTest {

    public static void main(String[] args) throws InterruptedException {
        final CassandraCounterFactory counterFactory = new CassandraCounterFactory()
                .setHostsAndPorts("localhost:9042").setKeyspace("demo")
                .setTableMetadata("tsc_metadata").init();
        final ICounter counter1 = counterFactory.getCounter("counter_1");
        // counter1.add(1);

        final int NUM_LOOPS = 20000;
        final int NUM_THREADS = 8;
        final AtomicLong COUNTER = new AtomicLong();
        final CountDownLatch countDownLatach = new CountDownLatch(NUM_THREADS);
        final long timestampStart = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            Thread t = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_LOOPS; i++) {
                        // counterFactory.getCounter("counter_1").add(1);
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

        long duration = timestampEnd - timestampStart;
        long counter = COUNTER.get();
        System.out.println("[" + counter + "] writes in [" + duration + " ms].");
        System.out.println(COUNTER.get() * 1000.0 / duration);

        counterFactory.destroy();
    }
}
