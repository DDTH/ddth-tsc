package com.github.ddth.tsc.cassandra.internal;

import java.util.HashMap;
import java.util.Map;

import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;

/**
 * Counter metadata.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class CounterMetadata {

    private final static String KEY_TABLE = "table";
    private final static String KEY_COUNTER_COLUMN = "counter_column";

    private String name;
    private String table = CqlTemplate.TABLE_COUNTER;
    private boolean isCounterColumn = true;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean isCounterColumn() {
        return isCounterColumn;
    }

    public void setCounterColumn(boolean isCounterColumn) {
        this.isCounterColumn = isCounterColumn;
    }

    /**
     * @return
     * @since 0.7.0
     */
    public String toJson() {
        return SerializationUtils.toJsonString(toMap());
    }

    /**
     * Creates a {@link CounterMetadata} object from a JSON string.
     * 
     * @param jsonString
     * @return
     */
    @SuppressWarnings("unchecked")
    public static CounterMetadata fromJsonString(String jsonString) {
        try {
            return fromMap(SerializationUtils.fromJsonString(jsonString, Map.class));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * @return
     * @since 0.7.0
     */
    @SuppressWarnings("serial")
    public Map<String, Object> toMap() {
        return new HashMap<String, Object>() {
            {
                put(KEY_TABLE, table);
                put(KEY_COUNTER_COLUMN, isCounterColumn);
            }
        };
    }

    /**
     * Creates a {@link CounterMetadata} object from a Map.
     * 
     * @param data
     * @return
     */
    public static CounterMetadata fromMap(Map<String, Object> data) {
        if (data == null) {
            return null;
        }
        CounterMetadata metadata = new CounterMetadata();
        Boolean boolValue = DPathUtils.getValue(data, KEY_COUNTER_COLUMN, Boolean.class);
        metadata.isCounterColumn = boolValue != null ? boolValue.booleanValue() : false;
        metadata.table = DPathUtils.getValue(data, KEY_TABLE, String.class);
        return metadata;
    }
}
