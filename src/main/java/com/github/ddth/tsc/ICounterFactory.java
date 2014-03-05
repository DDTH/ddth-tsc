package com.github.ddth.tsc;

/**
 * Factory to create {@link ICounter} instances.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public interface ICounterFactory {

    /**
     * Gets or Creates a counter instance.
     * 
     * @param name
     * @return
     */
    public ICounter getCounter(String name);

}
