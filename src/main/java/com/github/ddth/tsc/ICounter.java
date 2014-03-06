package com.github.ddth.tsc;

/**
 * Represent a time series counter.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public interface ICounter {

    public final static int RESOLUTION_MS = 1000; // 1 sec

    public final static int STEPS_1_SEC = RESOLUTION_MS;
    public final static int STEPS_5_SECS = 5 * RESOLUTION_MS;
    public final static int STEPS_10_SECS = 10 * RESOLUTION_MS;
    public final static int STEPS_15_SECS = 15 * RESOLUTION_MS;
    public final static int STEPS_30_SECS = 30 * RESOLUTION_MS;
    public final static int STEPS_1_MIN = 60 * RESOLUTION_MS;
    public final static int STEPS_5_MINS = 5 * 60 * RESOLUTION_MS;
    public final static int STEPS_10_MINS = 10 * 60 * RESOLUTION_MS;
    public final static int STEPS_15_MINS = 15 * 60 * RESOLUTION_MS;
    public final static int STEPS_30_MINS = 30 * 60 * RESOLUTION_MS;
    public final static int STEPS_1_HOUR = 60 * 60 * RESOLUTION_MS;

    public final static int LAST_SEC = 1;
    public final static int LAST_MIN = 60;
    public final static int LAST_HOUR = 60 * 60;
    public final static int LAST_DAY = 24 * 60 * 60;

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

    /**
     * Gets last N data points with step of 1.
     * 
     * @param n
     * @return
     */
    public DataPoint[] getLastN(int n);

    /**
     * Gets last N data points with specified steps.
     * 
     * @param n
     * @param steps
     * @return
     */
    public DataPoint[] getLastN(int n, int steps);
}
