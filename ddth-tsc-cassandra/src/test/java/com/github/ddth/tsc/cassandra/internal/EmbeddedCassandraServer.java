package com.github.ddth.tsc.cassandra.internal;

import java.io.File;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedCassandraServer {

    private Logger logger = LoggerFactory.getLogger(EmbeddedCassandraServer.class);

    private String baseDirectory = "target/Cassandra";
    private CassandraDaemon cassandraDaemon;
    private Thread cassandraThread;

    public String getBaseDirectory() {
        return baseDirectory;
    }

    public void start() throws Exception {
        try {
            cleanupDirectoriesFailover();

            FileUtils.createDirectory(baseDirectory);

            // System.setProperty("log4j.configuration",
            // "file:target/test-classes/log4j.properties");
            System.setProperty("cassandra.config", "file:target/test-classes/cassandra.yaml");

            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.init(null);
            cassandraThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        cassandraDaemon.start();
                    } catch (Exception e) {
                        logger.error("Embedded casandra server run failed", e);
                    }
                }
            });
            cassandraThread.setDaemon(true);
            cassandraThread.start();
        } catch (Exception e) {
            logger.error("Embedded casandra server start failed", e);
            stop();
        }
    }

    public void stop() throws Exception {
        if (cassandraThread != null) {
            cassandraDaemon.nativeServer.stop();
            cassandraDaemon.thriftServer.stop();
            cassandraDaemon.stop();
            cassandraDaemon.destroy();

            cassandraThread.interrupt();
            cassandraThread = null;

            // StorageService.optionalTasks.shutdownNow();
            // StorageService.scheduledTasks.shutdownNow();
            // StorageService.tasks.shutdownNow();
            // StorageService.instance.stopClient();
            // StorageService.instance.stopGossiping();
            // StorageService.instance.stopNativeTransport();
            // StorageService.instance.stopRPCServer();

            // ThreadGroup tg = Thread.currentThread().getThreadGroup();
            // Thread[] threads = new Thread[tg.activeCount()];
            // tg.enumerate(threads);
            // for (Thread t : threads) {
            // if (t.getName().equals("COMMIT-LOG-ALLOCATOR")
            // || t.getName().equals("COMMIT-LOG-WRITER")) {
            // t.interrupt();
            // }
            // }
        }

        cleanupDirectoriesFailover();
    }

    public void cleanupDirectoriesFailover() {
        int tries = 3;
        while (tries-- > 0) {
            try {
                cleanupDirectories();
                break;
            } catch (Exception e) {
                // ignore exception
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e1) {
                    // ignore exception
                }
            }
        }
    }

    public void cleanupDirectories() throws Exception {
        File dirFile = new File(baseDirectory);
        if (dirFile.exists()) {
            FileUtils.deleteRecursive(dirFile);
        }
    }
}
