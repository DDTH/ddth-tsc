package com.github.ddth.tsc;

import java.io.Serializable;

/**
 * Captures a single time series data point.
 * 
 * <p>
 * A {@link DataPoint} object captures the following information:
 * </p>
 * <ul>
 * <li>{@code timestamp}: UNIX timestamp (in milliseconds), where the data is
 * captured.</li>
 * <li>{@code blockSize}: size in milliseconds</code>
 * <li>{@code value}: value of the data point, aggregation of all time series
 * values in range [{@code timestamp}, {@code timestamp+blockSize}). See
 * {@link Type}.</li>
 * <li>{@code type}: how the values are aggregated. See {@link Type}.</li>
 * </ul>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class DataPoint implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public final static DataPoint[] EMPTY_ARR = new DataPoint[0];

    /**
     * Data point type:
     * 
     * <ul>
     * <li>{@code SUM}: sum of all values</li>
     * <li>{@code MINIMUM}: min of all values</li>
     * <li>{@code MAX}: max of all values</li>
     * <li>{@code AVERAGE}: average of all values</li>
     * <li>{@code NONE}: a special marker, indicating that the data point has no
     * value</li>
     * </ul>
     * 
     * @since 0.3.0
     */
    public static enum Type {
        SUM, MINIMUM, MAXIMUM, AVERAGE, NONE
    }

    private long timestamp;
    private long value, numPoints;
    private long blockSize;
    private Type type = Type.SUM;

    public DataPoint() {
    }

    public DataPoint(long timestampMs, long value, long blockSizeMs) {
        timestamp(timestampMs);
        set(value);
        blockSize(blockSizeMs);
    }

    public DataPoint(Type type, long timestampMs, long value, long blockSizeMs) {
        type(type);
        timestamp(timestampMs);
        set(value);
        blockSize(blockSizeMs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataPoint clone() throws CloneNotSupportedException {
        return (DataPoint) super.clone();
    }

    /**
     * Getter
     * 
     * @return
     * @since 0.3.0
     */
    public Type type() {
        return type;
    }

    /**
     * Setter
     * 
     * @param type
     * @return
     * @since 0.3.0
     */
    public DataPoint type(Type type) {
        if (this.type != type) {
            this.type = type;
            switch (type) {
            case AVERAGE:
            case SUM:
                this.value = 0;
                break;
            case MAXIMUM:
                this.value = Long.MIN_VALUE;
                break;
            case MINIMUM:
                this.value = Long.MAX_VALUE;
                break;
            default:
                this.value = 0;
                break;
            }
            this.numPoints = 0;
        }
        return this;
    }

    /**
     * Getter
     * 
     * @return
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Setter
     * 
     * @param timestampMs
     * @return
     */
    public DataPoint timestamp(long timestampMs) {
        this.timestamp = timestampMs;
        return this;
    }

    /**
     * Getter
     * 
     * @return
     */
    public long blockSize() {
        return blockSize;
    }

    /**
     * Setter
     * 
     * @param blockSizeMs
     * @return
     */
    public DataPoint blockSize(long blockSizeMs) {
        this.blockSize = blockSizeMs;
        return this;
    }

    private void _cloneFrom(DataPoint another) {
        value = another.value;
        numPoints = another.numPoints;
        type = another.type;
    }

    /**
     * Adds value from another data point.
     * 
     * @param another
     * @return
     * @since 0.3.0
     */
    public DataPoint add(DataPoint another) {
        if (type == Type.NONE) {
            _cloneFrom(another);
        } else {
            if (another.type != Type.NONE) {
                add(another.value());
            }
        }

        return this;
    }

    /**
     * Adds a value to the data point.
     * 
     * <p>
     * How the value is actually added depends on type of the data point. See
     * {@link Type}.
     * </p>
     * 
     * @param value
     * @return
     */
    public DataPoint add(long value) {
        switch (type) {
        case MINIMUM:
            this.value = Math.min(this.value, value);
            break;
        case MAXIMUM:
            this.value = Math.max(this.value, value);
            break;
        case AVERAGE:
        case SUM:
            this.value += value;
            this.numPoints++;
            break;
        default:
            throw new IllegalStateException("Unknown type [" + type + "]!");
        }
        return this;
    }

    /**
     * Sets data point's value using another data point.
     * 
     * @param another
     * @return
     * @since 0.3.0
     */
    public DataPoint set(DataPoint another) {
        if (type == Type.NONE) {
            _cloneFrom(another);
        } else {
            if (another.type != Type.NONE) {
                set(another.value());
            }
        }

        return this;
    }

    /**
     * Sets data point's value to a specific value.
     * 
     * @param value
     * @return
     */
    public DataPoint set(long value) {
        this.value = value;
        this.numPoints = 1;
        return this;
    }

    /**
     * Gets the data point value.
     * 
     * <p>
     * The returned value depends on type of data point's. See {@link Type}.
     * </p>
     * 
     * @return
     */
    public long value() {
        switch (type) {
        case AVERAGE:
            return numPoints != 0 ? value / numPoints : 0;
        case NONE:
            return 0;
        case MAXIMUM:
        case MINIMUM:
        case SUM:
            return value;
        default:
            throw new IllegalStateException("Unknown type [" + type + "]!");
        }
    }
}
