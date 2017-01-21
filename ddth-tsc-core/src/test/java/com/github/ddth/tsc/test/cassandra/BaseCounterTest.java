package com.github.ddth.tsc.test.cassandra;

import java.lang.reflect.Field;

import org.apache.cassandra.service.CassandraDaemon;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;

import com.datastax.driver.core.Session;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.ICounter;
import com.github.ddth.tsc.ICounterFactory;
import com.github.ddth.tsc.cassandra.CassandraCounterFactory;
import com.github.ddth.tsc.cassandra.internal.CqlTemplate;

import junit.framework.TestCase;

/**
 * Base class for test cases.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class BaseCounterTest extends TestCase {

    protected ICounter counterAdd, counterSet;
    protected ICounterFactory counterFactory;
    private SessionManager sessionManager;

    protected final static String COUNTER_NAME_ADD = CqlTemplate.TABLE_COUNTER + "_add";
    protected final static String COUNTER_NAME_SET = CqlTemplate.TABLE_COUNTER + "_set";

    public BaseCounterTest(String testName) {
        super(testName);
    }

    protected ICounterFactory createCounterFactory() throws Exception {
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(30000);
            Session session = EmbeddedCassandraServerHelper.getSession();
            session.execute(
                    "CREATE KEYSPACE tsc WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':'1'}");
            session.execute("CREATE TABLE tsc." + CqlTemplate.TABLE_METADATA
                    + " (c varchar, o text, PRIMARY KEY (c)) WITH COMPACT STORAGE");

            // counter 1: counter column
            String table1 = COUNTER_NAME_ADD;
            session.execute("UPDATE tsc." + CqlTemplate.TABLE_METADATA + " SET o='{\"table\":\""
                    + table1 + "\", \"counter_column\":true}' WHERE c='" + table1 + "'");
            session.execute("CREATE TABLE tsc." + table1
                    + " (c varchar, ym int, d int, t bigint, v counter, PRIMARY KEY ((c, ym, d), t) ) WITH COMPACT STORAGE");

            // counter 2: bigint column
            String table2 = COUNTER_NAME_SET;
            session.execute("UPDATE tsc." + CqlTemplate.TABLE_METADATA + " SET o='{\"table\":\""
                    + table2 + "\", \"counter_column\":false}' WHERE c='" + table2 + "'");
            session.execute("CREATE TABLE tsc." + table2
                    + " (c varchar, ym int, d int, t bigint, v bigint, PRIMARY KEY ((c, ym, d), t) ) WITH COMPACT STORAGE");

            sessionManager = new SessionManager();
            sessionManager.init();
            return new CassandraCounterFactory().setSessionManager(sessionManager)
                    .setHostsAndPorts("127.0.0.1:9142").setKeyspace("tsc").init();
        } catch (Exception e) {
            tearDown();
            throw e;
        }
    }

    @Before
    public void setUp() throws Exception {
        counterFactory = createCounterFactory();
        counterAdd = counterFactory.getCounter(COUNTER_NAME_ADD);
        counterSet = counterFactory.getCounter(COUNTER_NAME_SET);
    }

    @After
    public void tearDown() {
        try {
            if (counterFactory instanceof AbstractCounterFactory) {
                ((AbstractCounterFactory) counterFactory).destroy();
                counterFactory = null;
            }
        } catch (Exception e) {
        }

        try {
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        } catch (Exception e) {
        } finally {
        }

        try {
            EmbeddedCassandraServerHelper.getSession().close();
            EmbeddedCassandraServerHelper.getCluster().close();
        } catch (Exception e) {
        } finally {
        }

        try {
            Field f = EmbeddedCassandraServerHelper.class.getDeclaredField("cassandraDaemon");
            f.setAccessible(true);
            CassandraDaemon cassandraDaemon = (CassandraDaemon) f
                    .get(EmbeddedCassandraServerHelper.class);
            f.set(EmbeddedCassandraServerHelper.class, null);

            f = CassandraDaemon.class.getDeclaredField("runManaged");
            f.setAccessible(true);
            f.set(cassandraDaemon, true);

            cassandraDaemon.deactivate();
        } catch (Exception e) {
        } finally {
        }
    }

}
