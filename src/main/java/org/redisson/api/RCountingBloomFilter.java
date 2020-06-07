package org.redisson.api;

/**
 * Distributed implementation of Counting Bloom filter based on Highway 128-bit hash.
 *
 * @param <T> - type of object
 * @author linfeng
 */
public interface RCountingBloomFilter<T> extends RExpirable {

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

    /**
     * Initializes Bloom filter params (size and hashIterations)
     * calculated from <code>expectedInsertions</code> and <code>falseProbability</code>
     * Stores config to Redis server.
     *
     * @param expectedInsertions - expected amount of insertions per element
     * @param falseProbability   - expected false probability
     * @return <code>true</code> if Bloom filter initialized
     * <code>false</code> if Bloom filter already has been initialized
     */
    boolean tryInit(long expectedInsertions, double falseProbability);

    /**
     * Returns expected amount of insertions per element.
     * Calculated during bloom filter initialization.
     *
     * @return expected amount of insertions per element
     */
    long getExpectedInsertions();

    /**
     * Returns false probability of element presence.
     * Calculated during bloom filter initialization.
     *
     * @return false probability of element presence
     */
    double getFalseProbability();

    /**
     * Returns number of bits in Redis memory required by this instance
     *
     * @return number of bits
     */
    long getSize();

    /**
     * Returns hash iterations amount used per element.
     * Calculated during bloom filter initialization.
     *
     * @return hash iterations amount
     */
    int getHashIterations();

    /**
     * Calculates probabilistic number of elements already added to Bloom filter.
     *
     * @return probabilistic number of elements
     */
    long count();

}
