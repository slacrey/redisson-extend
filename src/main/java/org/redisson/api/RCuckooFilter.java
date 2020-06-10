package org.redisson.api;

public interface RCuckooFilter<T> extends RExpirable {

    /**
     * Adds element
     *
     * @param object - element to add
     * @return <code>true</code> if element has been added successfully
     * <code>false</code> if element is already present
     */
    boolean add(T object);

    /**
     * Check for element present
     *
     * @param object - element
     * @return <code>true</code> if element is present
     * <code>false</code> if element is not present
     */
    boolean contains(T object);

    /**
     * @param object - element
     * @return <code>true</code> element is deleted
     * <code>false</code> element is not deleted
     */
    boolean remove(T object);


}
