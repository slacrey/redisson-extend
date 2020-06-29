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
        RCountingBloomFilter<String> bloomFilter = redissonClientExtend.getCountingBloomFilter("test2", 3);

        //90,000,000
        bloomFilter.delete();
        bloomFilter.tryInit(50000000, 0.0000001D);

        long startTime = System.currentTimeMillis();
        for (int i=0;i<100000;i++) {
            bloomFilter.add("test1" + i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("add time:" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for (int i=0; i< 100000; i++) {
            bloomFilter.contains("test1" + i);
        }
        endTime = System.currentTimeMillis();

        System.out.println("contains time:" + (endTime - startTime));
        System.out.println("remove:" + bloomFilter.remove("test1"));
        System.out.println("remove:" + bloomFilter.remove("test1"));
        System.out.println("contains:" + bloomFilter.contains("test1"));

    }

}
