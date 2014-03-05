package com.github.ddth.tsc;

/**
 * Represent a time series counter.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public interface ICounter {

    /**
     * Adds a value, uses {@code System.currentTimeMillis()} as key.
     * 
     * @param value
     */
    public void add(long value);

    /**
     * Adds a value, key specified by the supplied timestamp.
     * 
     * @param timestampMs
     *            UNIX timestamp in millisec
     * @param value
     */
    public void add(long timestampMs, long value);

    /**
     * Gets single counter value at a specific time specified by
     * {@code timestampMs}.
     * 
     * @param timestampMs
     * @return
     * @see #getSeries(long, long, int)
     */
    public DataPoint get(long timestampMs);

    /**
     * Gets time series data from {@code timestampStartMs} to
     * {@link System#currentTimeMillis()} with step of 1.
     * 
     * @param timestampStartMs
     * @return
     * @see #getSeries(long, long, int)
     */
    public DataPoint[] getSeries(long timestampStartMs);

    /**
     * Gets time series data from {@code timestampStartMs} to
     * {@link System#currentTimeMillis()} with specified steps.
     * 
     * @param timestampStartMs
     * @param steps
     * @return
     * @see #getSeries(long, long, int)
     */
    public DataPoint[] getSeries(long timestampStartMs, int steps);

    /**
     * Gets time series data from {@code timestampStartMs} to
     * {@code timestampEndMs} with step of 1.
     * 
     * @param timestampStartMs
     * @param timestampEndMs
     * @return
     */
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs);

    /**
     * Gets time series data from {@code timestampStartMs} (inclusive) to
     * {@code timestampEndMs} (exclusive) with specified steps.
     * 
     * <ul>
     * <li>{@code timestampStartMs} must be less than or equals to
     * {@code timestampEndMs} or an {@link IllegalArgumentException} is thrown.</li>
     * </ul>
     * 
     * @param timestampStartMs
     * @param timestampEndMs
     * @param steps
     *            in seconds
     * @return
     */
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs, int steps);
}
