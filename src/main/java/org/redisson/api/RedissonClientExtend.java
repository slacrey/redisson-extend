package org.redisson.api;

import org.redisson.client.codec.Codec;

/**
 * @author linfeng
 * @date 2020-06-06
 **/
public interface RedissonClientExtend extends RedissonClient {


    /**
     * Returns counting bloom filter instance by name.
     *
     * @param <V>  type of value
     * @param name - name of object
     * @return CountingBloomFilter object
     */
    <V> RCountingBloomFilter<V> getCountingBloomFilter(String name);

    /**
     * Returns counting bloom filter instance by name.
     *
     * @param name   - name of object
     * @param repeat - repeat number of object
     * @param <V>    type of value
     * @return CountingBloomFilter object
     */
    <V> RCountingBloomFilter<V> getCountingBloomFilter(String name, int repeat);

    /**
     * Returns counting bloom filter instance by name
     * using provided codec for objects.
     *
     * @param <V>   type of value
     * @param name  - name of object
     * @param codec - codec for values
     * @return CountingBloomFilter object
     */
    <V> RCountingBloomFilter<V> getCountingBloomFilter(String name, Codec codec);


    /**
     * Returns counting bloom filter instance by name
     * using provided codec for objects.
     *
     * @param name   - name of object
     * @param repeat - repeat number of object
     * @param codec  - codec for values
     * @param <V>    type of value
     * @return CountingBloomFilter object
     */
    <V> RCountingBloomFilter<V> getCountingBloomFilter(String name, int repeat, Codec codec);
}
