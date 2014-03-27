package com.github.ddth.tsc;

/**
 * Represent a time series counter.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public interface ICounter {

    public final static int RESOLUTION_MS = 1000; // 1 sec

    public final static int STEPS_1_SEC = 1;
    public final static int STEPS_5_SECS = 5 * STEPS_1_SEC;
    public final static int STEPS_10_SECS = 10 * STEPS_1_SEC;
    public final static int STEPS_15_SECS = 15 * STEPS_1_SEC;
    public final static int STEPS_30_SECS = 30 * STEPS_1_SEC;
    public final static int STEPS_1_MIN = 60 * STEPS_1_SEC;
    public final static int STEPS_5_MINS = 5 * 60 * STEPS_1_SEC;
    public final static int STEPS_10_MINS = 10 * 60 * STEPS_1_SEC;
    public final static int STEPS_15_MINS = 15 * 60 * STEPS_1_SEC;
    public final static int STEPS_30_MINS = 30 * 60 * STEPS_1_SEC;
    public final static int STEPS_1_HOUR = 60 * 60 * STEPS_1_SEC;

    public final static int LAST_SEC = 1;
    public final static int LAST_MIN = 60;
    public final static int LAST_HOUR = 60 * 60;
    public final static int LAST_DAY = 24 * 60 * 60;

    /**
     * Gets counter name.
     * 
     * @return
     * @since 0.3.0
     */
    public String getName();

    /**
     * Adds a value to data point at {@code System.currentTimeMillis()}.
     * 
     * @param value
     * @return
     */
    public void add(long value);

    /**
     * Adds a value to data point at {@code timestampMs}.
     * 
     * @param timestampMs
     *            UNIX timestamp in millisec
     * @param value
     */
    public void add(long timestampMs, long value);

    /**
     * Sets a value to data point at {@code System.currentTimeMillis()}.
     * 
     * @param value
     * @since 0.3.0
     */
    public void set(long value);

    /**
     * Sets a value to data point at {@code timestampMs}.
     * 
     * @param timestampMs
     * @param value
     * @since 0.3.0
     */
    public void set(long timestampMs, long value);

    /**
     * Gets single counter value at a specific time specified by
     * {@code timestampMs} with step of 1.
     * 
     * @param timestampMs
     * @return
     * @see #getSeries(long, long, int)
     */
    public DataPoint get(long timestampMs);

    /**
     * Gets single aggregated counter value at a specific time specified by
     * {@code timestampMs} with specified steps.
     * 
     * @param timestampMs
     * @param type
     * @param steps
     * @return
     * @see #getSeries(long, long, int, DataPoint.Type)
     */
    public DataPoint get(long timestampMs, DataPoint.Type type, int steps);

    /**
     * Gets time series data in range [{@code timestampStartMs},
     * {@link System#currentTimeMillis()}) with step of 1.
     * 
     * @param timestampStartMs
     * @return
     * @see #getSeries(long, long, int)
     */
    public DataPoint[] getSeries(long timestampStartMs);

    /**
     * Gets time series data in range [{@code timestampStartMs},
     * {@link System#currentTimeMillis()}) with step of 1.
     * 
     * @param timestampStartMs
     * @param type
     * @return
     * @since 0.3.0
     * @see #getSeries(long, long, int, DataPoint.Type)
     */
    public DataPoint[] getSeries(long timestampStartMs, DataPoint.Type type);

    /**
     * Gets time series data in range [{@code timestampStartMs},
     * {@link System#currentTimeMillis()}) with specified steps.
     * 
     * @param timestampStartMs
     * @param steps
     * @return
     * @see #getSeries(long, long, int)
     */
    public DataPoint[] getSeries(long timestampStartMs, int steps);

    /**
     * Gets time series data in range [{@code timestampStartMs},
     * {@link System#currentTimeMillis()}) with specified steps.
     * 
     * @param timestampStartMs
     * @param steps
     * @param type
     * @return
     * @since 0.3.0
     * @see #getSeries(long, long, int, DataPoint.Type)
     */
    public DataPoint[] getSeries(long timestampStartMs, int steps, DataPoint.Type type);

    /**
     * Gets time series data in range [{@code timestampStartMs},
     * {@code timestampEndMs}) with step of 1.
     * 
     * @param timestampStartMs
     * @param timestampEndMs
     * @return
     * @see #getSeries(long, long, int)
     */
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs);

    /**
     * Gets time series data in range [{@code timestampStartMs},
     * {@code timestampEndMs}) with step of 1.
     * 
     * @param timestampStartMs
     * @param timestampEndMs
     * @param type
     * @return
     * @since 0.3.0
     * @see #getSeries(long, long, int, DataPoint.Type)
     */
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs, DataPoint.Type type);

    /**
     * Gets time series data in range [{@code timestampStartMs},
     * {@code timestampEndMs}) with specified steps.
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
     * Gets time series data in range [{@code timestampStartMs},
     * {@code timestampEndMs}) with specified steps.
     * 
     * <ul>
     * <li>{@code timestampStartMs} must be less than or equals to
     * {@code timestampEndMs} or an {@link IllegalArgumentException} is thrown.</li>
     * <li>{@code type} defines now data point values are aggregated. See
     * {@link DataPoint.Type} for more information.</li>
     * </ul>
     * 
     * @param timestampStartMs
     * @param timestampEndMs
     * @param steps
     * @param type
     * @return
     * @since 0.3.0
     */
    public DataPoint[] getSeries(long timestampStartMs, long timestampEndMs, int steps,
            DataPoint.Type type);

    /**
     * Gets last N data points with step of 1.
     * 
     * @param n
     * @return
     */
    public DataPoint[] getLastN(int n);

    /**
     * Gets last N data points with step of 1.
     * 
     * @param n
     * @param type
     * @return
     * @since 0.3.0
     */
    public DataPoint[] getLastN(int n, DataPoint.Type type);

    /**
     * Gets last N data points with specified steps.
     * 
     * @param n
     * @param steps
     * @return
     */
    public DataPoint[] getLastN(int n, int steps);

    /**
     * Gets last N data points with specified steps.
     * 
     * @param n
     * @param steps
     * @param type
     * @return
     * @since 0.3.0
     */
    public DataPoint[] getLastN(int n, int steps, DataPoint.Type type);
}
