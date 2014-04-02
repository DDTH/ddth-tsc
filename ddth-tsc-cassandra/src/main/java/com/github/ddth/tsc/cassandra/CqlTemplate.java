package com.github.ddth.tsc.cassandra;

/**
 * Templates for commonly used CQL.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class CqlTemplate {

    public final static String TABLE_METADATA = "tsc_metadata";
    public final static String TABLE_COUNTER = "tsc_counters";

    public final static String COL_COUNTER_COUNTER = "c";
    public final static String COL_COUNTER_YEAR_MONTH = "ym";
    public final static String COL_COUNTER_DAY = "d";
    public final static String COL_COUNTER_TIMESTAMP = "t";
    public final static String COL_COUNTER_VALUE = "v";

    public final static String CQL_TEMPLATE_ADD_COUNTER = "UPDATE {0} SET v=v+? WHERE c=? AND ym=? AND d=? AND t=?";
    public final static String CQL_TEMPLATE_SET_COUNTER = "UPDATE {0} SET v=? WHERE c=? AND ym=? AND d=? AND t=?";
    public final static String CQL_TEMPLATE_GET_COUNTER = "SELECT c,ym,d,t,v FROM {0} WHERE c=? AND ym=? AND d=? AND t=?";
    public final static String CQL_TEMPLATE_GET_COUNTER_ROW = "SELECT c,ym,d,t,v FROM {0} WHERE c=? AND ym=? AND d=?";
}
