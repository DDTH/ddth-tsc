package com.github.ddth.tsc.cassandra.internal;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.cql.CqlUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Counter metadata manager.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class MetadataManager {

    public final static String DEFAULT_METADATA_TABLE = "tsc_metadata";
    private final static String DEFAULT_METADATA_NAME = "*";

    private SessionHelper sessionHelper;
    private String tableMetadata = MetadataManager.DEFAULT_METADATA_TABLE;
    private String cqlGetMetadata;

    public MetadataManager() {
    }

    public MetadataManager(SessionHelper sessionHelper) {
        this.sessionHelper = sessionHelper;
    }

    /**
     * @return
     * @since 0.7.0
     */
    public SessionHelper getSessionHelper() {
        return sessionHelper;
    }

    /**
     * @param sessionHelper
     * @return
     * @since 0.7.0
     */
    public MetadataManager setSessionHelper(SessionHelper sessionHelper) {
        this.sessionHelper = sessionHelper;
        return this;
    }

    /**
     * Name of the table to store counter metadata.
     * 
     * @param tableMetadata
     * @return
     */
    public MetadataManager setTableMetadata(String tableMetadata) {
        this.tableMetadata = tableMetadata;
        return this;
    }

    protected String getTableMetadata() {
        return tableMetadata;
    }

    public MetadataManager init() {
        int numProcessors = Runtime.getRuntime().availableProcessors();
        cache = CacheBuilder.newBuilder().concurrencyLevel(numProcessors)
                .expireAfterWrite(5, TimeUnit.MINUTES).build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        return _read(key);
                    }
                });
        cqlGetMetadata = MessageFormat.format(CqlTemplate.CQL_TEMPLATE_GET_METADATA, tableMetadata);

        return this;
    }

    public void destroy() {
        cache.invalidateAll();
    }

    /*----------------------------------------------------------------------*/

    private LoadingCache<String, String> cache;

    /**
     * Reads a row data from storage (no cache).
     * 
     * @param rowKey
     * @return
     * @throws RowNotFoundException
     */
    private String _read(String rowKey) throws RowNotFoundException {
        Row row = CqlUtils.executeOne(sessionHelper.getSession(), cqlGetMetadata,
                ConsistencyLevel.LOCAL_ONE, rowKey);
        String jsonString = row != null ? row.getString(CqlTemplate.COL_METADATA_METADATA) : null;
        if (jsonString != null) {
            return jsonString;
        }
        throw new RowNotFoundException(rowKey);
    }

    /**
     * Reads a row data from storage (cache supported).
     * 
     * @param rowKey
     * @return
     */
    private String getRow(String rowKey) {
        try {
            return cache.get(rowKey);
        } catch (Exception e) {
            if (e.getCause() instanceof RowNotFoundException) {
                return null;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private CounterMetadata findCounterMetadata(String counterName) {
        String jsonString = getRow(DEFAULT_METADATA_NAME);
        List<Object> parent = SerializationUtils.fromJsonString(jsonString, List.class);
        if (parent == null) {
            return null;
        }
        for (Object entry : parent) {
            if (entry instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) entry;
                String pattern = DPathUtils.getValue(map, "pattern", String.class);
                if (Pattern.matches(pattern, counterName)) {
                    return CounterMetadata.fromMap(map);
                }
            }
        }
        return null;
    }

    /**
     * Gets a counter metadata by name.
     * 
     * @param name
     * @return
     */
    public CounterMetadata getCounterMetadata(String counterName) {
        // first fetch metadata for the specified counter
        String jsonString = getRow(counterName);
        if (jsonString != null) {
            CounterMetadata result = CounterMetadata.fromJsonString(jsonString);
            if (result != null) {
                result.setName(counterName);
            }
            return result;
        } else {
            // no exact match, try to build counter metadata from global config
            return findCounterMetadata(counterName);
        }
    }
    /*----------------------------------------------------------------------*/
}
