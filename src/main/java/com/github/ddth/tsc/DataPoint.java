package com.github.ddth.tsc;

/**
 * Encapsulates a single time series data point.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class DataPoint {

    public final static DataPoint[] EMPTY_COUNTER_BLOCK_ARR = new DataPoint[0];

    private long timestamp;
    private long value;
    private long blockSize;

    public DataPoint(long timestamp, long value, long blockSize) {
        this.timestamp = timestamp;
        this.value = value;
        this.blockSize = blockSize;
    }

    public long timestamp() {
        return timestamp;
    }

    public long blockSize() {
        return blockSize;
    }

    public void add(long value) {
        this.value += value;
    }

    public void set(long value) {
        this.value = value;
    }

    public long value() {
        return value;
    }
}
