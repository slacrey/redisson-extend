package org.redisson;

import io.netty.buffer.ByteBuf;
import org.redisson.api.MapOptions;
import org.redisson.api.RCuckooFilter;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.Hash;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.math.DoubleMath.log2;
import static com.google.common.math.LongMath.divide;
import static java.lang.Math.ceil;
import static java.lang.Math.pow;
import static java.math.RoundingMode.CEILING;
import static java.math.RoundingMode.HALF_DOWN;

public class RedissonCuckooFilter<K, V> extends RedissonExpirable implements RCuckooFilter<V> {

    static double MIN_FPP = 1.0D / pow(2, 60);
    static double MAX_FPP = 0.99D;
    static final int MAX_ENTRIES_PER_BUCKET = 8;
    static final int MIN_ENTRIES_PER_BUCKET = 2;

    private final RedissonMap<K, V> redissonOneMap;
    private final RedissonMap<K, V> redissonTwoMap;

    protected long numBuckets;
    protected int numEntriesPerBucket;
    protected int numBitsPerEntry;


    public RedissonCuckooFilter(CommandAsyncExecutor commandExecutor, String name,
                                RedissonClient redisson, MapOptions<K, V> options,
                                long capacity, double fpp) {
        super(commandExecutor, name);

        checkArgument(capacity > 0, "Expected insertions (%s) must be > 0", capacity);
        checkArgument(fpp > 0.0D, "False positive probability (%s) must be > 0.0", fpp);
        checkArgument(fpp < 1.0D, "False positive probability (%s) must be < 1.0", fpp);

        int numEntriesPerBucket = optimalEntriesPerBucket(fpp);
        long numBuckets = optimalNumberOfBuckets(capacity, numEntriesPerBucket);
        int numBitsPerEntry = optimalBitsPerEntry(fpp, numEntriesPerBucket);

        this.redissonOneMap = new RedissonMap(commandExecutor, suffixName(name, "map1"), redisson, options);
        this.redissonTwoMap = new RedissonMap(commandExecutor, suffixName(name, "map2"), redisson, options);

    }

    public RedissonCuckooFilter(Codec codec, CommandAsyncExecutor commandExecutor,
                                String name, RedissonClient redisson, MapOptions<K, V> options,
                                long capacity, double fpp) {
        super(commandExecutor, name);

        checkArgument(capacity > 0, "Expected insertions (%s) must be > 0", capacity);
        checkArgument(fpp > 0.0D, "False positive probability (%s) must be > 0.0", fpp);
        checkArgument(fpp < 1.0D, "False positive probability (%s) must be < 1.0", fpp);

        int numEntriesPerBucket = optimalEntriesPerBucket(fpp);
        long numBuckets = optimalNumberOfBuckets(capacity, numEntriesPerBucket);
        int numBitsPerEntry = optimalBitsPerEntry(fpp, numEntriesPerBucket);

        this.redissonOneMap = new RedissonMap(commandExecutor, suffixName(name, "map1"), redisson, options);
        this.redissonTwoMap = new RedissonMap(commandExecutor, suffixName(name, "map2"), redisson, options);
        
    }

    static int optimalBitsPerEntry(double fpp, int numEntriesPerBucket) {
        checkArgument(fpp >= MIN_FPP, "Cannot create CuckooFilter with FPP[" + fpp +
                "] < CuckooFilter.MIN_FPP[" + MIN_FPP + "]");
        return log2(2 * numEntriesPerBucket / fpp, HALF_DOWN);
    }

    static double optimalLoadFactor(int b) {
        checkArgument(b == 2 || b == 4 || b == 8, "b must be 2, 4, or 8");
        if (b == 2) {
            return 0.84D;
        } else if (b == 4) {
            return 0.955D;
        } else {
            return 0.98D;
        }
    }

    /**
     * 计算最有桶数量
     *
     * @param capacity
     * @param numEntriesPerBucket
     * @return
     */
    static long optimalNumberOfBuckets(long capacity, int numEntriesPerBucket) {
        checkArgument(capacity > 0, "capacity must be > 0");
        return evenCeil(divide((long) ceil(capacity / optimalLoadFactor(numEntriesPerBucket)), numEntriesPerBucket, CEILING));
    }

    static int optimalEntriesPerBucket(double fpp) {
        checkArgument(fpp > 0.0D, "fpp must be > 0.0");
        if (fpp <= 0.00001) {
            return MAX_ENTRIES_PER_BUCKET;
        } else if (fpp <= 0.002) {
            return MAX_ENTRIES_PER_BUCKET / 2;
        } else {
            return MIN_ENTRIES_PER_BUCKET;
        }
    }

    static long evenCeil(long n) {
        return (n + 1) / 2 * 2;
    }

    private long hash(Object object) {
        ByteBuf state = encode(object);
        try {
            return Hash.hash64(state);
        } finally {
            state.release();
        }
    }

    int fingerPrint(int hash, int unit) {

        int mask = (0x80000000 >> (unit - 1)) >>> (Long.SIZE - unit);

        for (int bit = 0; (bit + unit) <= Long.SIZE; bit += unit) {
            int ret = (hash >> bit) & mask;
            if (0 != ret) {
                return ret;
            }
        }
        return 0x1;
    }

    @Override
    public boolean add(V object) {

        final long hash64 = hash(object);
        final int hash1 = (int) hash64;
        final int hash2 = (int) (hash64 >>> 32);

        final int fp = fingerPrint(hash2, 10);

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
