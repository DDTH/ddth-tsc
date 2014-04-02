package com.github.ddth.tsc.cassandra.internal;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Cassandra utility class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class CassandraUtils {

    /**
     * Executes a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param bindValues
     */
    public static void executeNonSelect(Session session, String cql, Object... bindValues) {
        executeNonSelect(session, session.prepare(cql), bindValues);
    }

    /**
     * Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param bindValues
     */
    public static void executeNonSelect(Session session, PreparedStatement stm,
            Object... bindValues) {
        BoundStatement bstm = stm.bind();
        if (bindValues != null && bindValues.length > 0) {
            bstm.bind(bindValues);
        }
        session.execute(bstm);
    }

    /**
     * Executes a SELECT query and returns results.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     */
    public static ResultSet execute(Session session, String cql, Object... bindValues) {
        return execute(session, session.prepare(cql), bindValues);
    }

    /**
     * Executes a SELECT query and returns results.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     */
    public static ResultSet execute(Session session, PreparedStatement stm, Object... bindValues) {
        BoundStatement bstm = stm.bind();
        if (bindValues != null && bindValues.length > 0) {
            bstm.bind(bindValues);
        }
        return session.execute(bstm);
    }

    /**
     * Executes a SELECT query and returns just one row.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     */
    public static Row executeOne(Session session, String cql, Object... bindValues) {
        return executeOne(session, session.prepare(cql), bindValues);
    }

    /**
     * Executes a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     */
    public static Row executeOne(Session session, PreparedStatement stm, Object... bindValues) {
        ResultSet rs = execute(session, stm, bindValues);
        return rs != null ? rs.one() : null;
    }
}
