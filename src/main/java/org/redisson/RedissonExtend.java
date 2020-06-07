package org.redisson;

import org.redisson.api.RCountingBloomFilter;
import org.redisson.api.RedissonClientExtend;
import org.redisson.client.codec.Codec;
import org.redisson.config.Config;

/**
 * @author linfeng
 * @date 2020-06-06
 **/
public class RedissonExtend extends Redisson implements RedissonClientExtend {

    protected RedissonExtend(Config config) {
        super(config);
    }

    public static RedissonClientExtend create() {
        Config config = new Config();
        config.useSingleServer()
                .setTimeout(1000000)
                .setAddress("redis://127.0.0.1:6379");
        return create(config);
    }

    public static RedissonClientExtend create(Config config) {
        RedissonExtend redisson = new RedissonExtend(config);
        if (config.isReferenceEnabled()) {
            redisson.enableRedissonReferenceSupport();
        }

        return redisson;
    }

    @Override
    public <V> RCountingBloomFilter<V> getCountingBloomFilter(String name) {
        return new RedissonCountingBloomFilter<>(this.connectionManager.getCommandExecutor(), name);
    }

    @Override
    public <V> RCountingBloomFilter<V> getCountingBloomFilter(String name, int repeat) {
        return new RedissonCountingBloomFilter<>(this.connectionManager.getCommandExecutor(), name, repeat);
    }

    @Override
    public <V> RCountingBloomFilter<V> getCountingBloomFilter(String name, Codec codec) {
        return new RedissonCountingBloomFilter<>(codec, this.connectionManager.getCommandExecutor(), name);
    }

    @Override
    public <V> RCountingBloomFilter<V> getCountingBloomFilter(String name, int repeat, Codec codec) {
        return new RedissonCountingBloomFilter<>(codec, this.connectionManager.getCommandExecutor(), name, repeat);
    }
}
