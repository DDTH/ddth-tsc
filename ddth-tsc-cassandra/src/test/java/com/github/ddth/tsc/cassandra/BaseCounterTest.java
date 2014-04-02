package com.github.ddth.tsc.cassandra;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.github.ddth.tsc.cassandra.internal.EmbeddedCassandraServer;
import com.github.ddth.tsc.mem.InmemCounter;

/**
 * Test cases for {@link InmemCounter}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class BaseCounterTest extends TestCase {

    protected CassandraCounter counter1, counter2;
    protected CassandraCounterFactory counterFactory;
    protected EmbeddedCassandraServer embeddedCassandraServer;
    protected Cluster cluster;

    private final static String CASSANDRA_HOST = "127.0.0.1";
    private final static int CASSANDRA_PORT = 9042;
    private final static String KEYSPACE = "tsc";

    public BaseCounterTest(String testName) {
        super(testName);
    }

    @Before
    public void setUp() throws Exception {
        embeddedCassandraServer = new EmbeddedCassandraServer();
        embeddedCassandraServer.start();

        cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).withPort(CASSANDRA_PORT)
                .build();
        Session session = cluster.connect("system");
        session.execute("CREATE KEYSPACE "
                + KEYSPACE
                + " WITH replication={'class':'SimpleStrategy','replication_factor':'1'} AND durable_writes=true");

        // create metadata table
        session.execute("CREATE TABLE " + KEYSPACE + "." + CqlTemplate.TABLE_METADATA
                + " (c varchar, o text, PRIMARY KEY (c)) WITH COMPACT STORAGE");

        // counter 1: counter column
        String table1 = CqlTemplate.TABLE_COUNTER + "_1";
        session.execute("UPDATE " + KEYSPACE + "." + CqlTemplate.TABLE_METADATA
                + " SET o='{\"table\":\"" + table1
                + "\", \"counter_column\":true}' WHERE c='test_counter_1'");
        session.execute("CREATE TABLE "
                + KEYSPACE
                + "."
                + table1
                + " (c varchar, ym int, d int, t bigint, v counter, PRIMARY KEY ((c, ym, d), t) ) WITH COMPACT STORAGE");

        // counter 2: bigint column
        String table2 = CqlTemplate.TABLE_COUNTER + "_2";
        session.execute("UPDATE " + KEYSPACE + "." + CqlTemplate.TABLE_METADATA
                + " SET o='{\"table\":\"" + table2
                + "\", \"counter_column\":false}' WHERE c='test_counter_2'");
        session.execute("CREATE TABLE "
                + KEYSPACE
                + "."
                + table2
                + " (c varchar, ym int, d int, t bigint, v bigint, PRIMARY KEY ((c, ym, d), t) ) WITH COMPACT STORAGE");

        session.close();

        counterFactory = new CassandraCounterFactory();
        counterFactory.setCluster(cluster).setKeyspace(KEYSPACE);
        counterFactory.init();

        counter1 = (CassandraCounter) counterFactory.getCounter("test_counter_1");
        counter2 = (CassandraCounter) counterFactory.getCounter("test_counter_2");
    }

    @After
    public void tearDown() throws Exception {

        if (counterFactory != null) {
            try {
                counterFactory.destroy();
            } catch (Exception e) {
            } finally {
                counterFactory = null;
            }
        }

        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception e) {
            } finally {
                cluster = null;
            }
        }

        if (embeddedCassandraServer != null) {
            try {
                embeddedCassandraServer.stop();
            } catch (Exception e) {
            } finally {
                embeddedCassandraServer = null;
            }
        }
    }
}
