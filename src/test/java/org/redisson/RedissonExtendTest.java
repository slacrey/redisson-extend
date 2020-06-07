package org.redisson;


import org.redisson.api.RCountingBloomFilter;
import org.redisson.api.RedissonClientExtend;
import org.redisson.config.Config;

/**
 * @author linfeng
 * @date 2020-06-06
 **/
public class RedissonExtendTest {

    public static void main(String[] args) {

        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        RedissonClientExtend redissonClientExtend = RedissonExtend.create(config);


//        RBloomFilter<String> bloomFilter = redissonClientExtend.getBloomFilter("test1");
        RCountingBloomFilter<String> bloomFilter = redissonClientExtend.getCountingBloomFilter("test2", 9);

        bloomFilter.delete();
        bloomFilter.tryInit(100, 0.01D);
        System.out.println("add:" + bloomFilter.add("test1"));
        System.out.println("add:" + bloomFilter.add("test1"));
        System.out.println("add:" + bloomFilter.add("test1"));
        System.out.println("add:" + bloomFilter.add("test1"));
        System.out.println("remove:" + bloomFilter.remove("test1"));
        System.out.println("remove:" + bloomFilter.remove("test1"));
        System.out.println("remove:" + bloomFilter.remove("test1"));
//        System.out.println("remove:" + bloomFilter.remove("test1"));
        System.out.println("contains:" + bloomFilter.contains("test1"));

    }

}
