package com.github.ddth.tsc.cassandra.internal;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.tsc.cassandra.CqlTemplate;

/**
 * Counter metadata.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class CounterMetadata {

    private final static String KEY_TABLE = "table";
    private final static String KEY_COUNTER_COLUMN = "counter_column";

    @JsonIgnore
    public String name;

    @JsonProperty(KEY_TABLE)
    public String table = CqlTemplate.TABLE_COUNTER;

    @JsonProperty(KEY_COUNTER_COLUMN)
    public boolean isCounterColumn = true;

    /**
     * Creates a {@link CounterMetadata} object from a JSON string.
     * 
     * @param jsonString
     * @return
     */
    public static CounterMetadata fromJsonString(String jsonString) {
        try {
            return SerializationUtils.fromJsonString(jsonString, CounterMetadata.class);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Creates a {@link CounterMetadata} object from a Map.
     * 
     * @param data
     * @return
     */
    public static CounterMetadata fromMap(Map<String, Object> data) {
        CounterMetadata metadata = new CounterMetadata();
        Boolean boolValue = DPathUtils.getValue(data, KEY_COUNTER_COLUMN, Boolean.class);
        metadata.isCounterColumn = boolValue != null ? boolValue.booleanValue() : false;
        metadata.table = DPathUtils.getValue(data, KEY_TABLE, String.class);
        return metadata;
    }
}
