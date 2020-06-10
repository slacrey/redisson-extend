package org.redisson;

import org.redisson.api.MapOptions;
import org.redisson.api.RCuckooFilter;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.command.CommandAsyncExecutor;

public class RedissonCuckooFilter<K, V> extends RedissonExpirable implements RCuckooFilter<V> {

    private final RedissonMap<K, V> redissonMap;
    private final String configOne;
    private final String configTwo;

    public RedissonCuckooFilter(CommandAsyncExecutor commandExecutor, String name,
                                RedissonClient redisson, MapOptions<K, V> options) {
        super(commandExecutor, name);
        this.redissonMap = new RedissonMap(commandExecutor, name, redisson, options);
        this.configOne = suffixName(name, "map1");
        this.configTwo = suffixName(name, "map2");
    }

    public RedissonCuckooFilter(Codec codec, CommandAsyncExecutor commandExecutor,
                                String name, RedissonClient redisson, MapOptions<K, V> options) {
        super(commandExecutor, name);
        this.redissonMap = new RedissonMap(codec, commandExecutor, name, redisson, options);
        this.configOne = suffixName(name, "map1");
        this.configTwo = suffixName(name, "map2");
    }

    @Override
    public boolean add(V object) {
        return false;
    }

    @Override
    public boolean contains(V object) {
        return false;
    }

    @Override
    public boolean remove(V object) {
        return false;
    }


}
