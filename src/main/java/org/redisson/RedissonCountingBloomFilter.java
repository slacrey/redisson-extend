package org.redisson;

import io.netty.buffer.ByteBuf;
import org.redisson.api.RBitSetAsync;
import org.redisson.api.RCountingBloomFilter;
import org.redisson.api.RFuture;
import org.redisson.client.RedisException;
import org.redisson.client.codec.*;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.VoidReplayConvertor;
import org.redisson.client.protocol.decoder.ObjectMapReplayDecoder;
import org.redisson.command.CommandBatchService;
import org.redisson.command.CommandExecutor;
import org.redisson.misc.Hash;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.abs;

/**
 * Counting Bloom filter based on Highway 128-bit hash.
 *
 * @param <T> type of object
 * @author linfeng
 */
public class RedissonCountingBloomFilter<T> extends RedissonExpirable implements RCountingBloomFilter<T> {


    private int DEFAULT_MAX_REPEAT = 3;
    private Boolean[] defaultList;
    private volatile long size;
    // 最大计数
    private volatile int maxRepeat = DEFAULT_MAX_REPEAT;
    private volatile int maxBinaryBit;
    private volatile int hashIterations;

    private final CommandExecutor commandExecutor;
    private final String configName;

    protected RedissonCountingBloomFilter(CommandExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getName(), "config");
        this.maxBinaryBit = Integer.toBinaryString(maxRepeat).length();
        this.defaultList = defaultOneData();
    }

    protected RedissonCountingBloomFilter(CommandExecutor commandExecutor, String name, int repeat) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getName(), "config");
        this.maxRepeat = repeat;
        this.maxBinaryBit = Integer.toBinaryString(maxRepeat).length();
        this.defaultList = defaultOneData();
    }

    protected RedissonCountingBloomFilter(Codec codec, CommandExecutor commandExecutor, String name) {
        super(codec, commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getName(), "config");
        this.maxBinaryBit = Integer.toBinaryString(maxRepeat).length();
        this.defaultList = defaultOneData();
    }

    protected RedissonCountingBloomFilter(Codec codec, CommandExecutor commandExecutor, String name, int repeat) {
        super(codec, commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getName(), "config");
        this.maxRepeat = repeat;
        this.maxBinaryBit = Integer.toBinaryString(maxRepeat).length();
        this.defaultList = defaultOneData();
    }

    private int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    private long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private long[] hash(Object object) {
        ByteBuf state = encode(object);
        try {
            return Hash.hash128(state);
        } finally {
            state.release();
        }
    }

    @Override
    public boolean add(T object) {
        long[] hashes = hash(object);

        while (true) {

            Boolean[] entryData;
            ReadResult readResult = readData(hashes);
            if (readResult != null && readResult.success()) {
                // 获取已经+1的数组
                entryData = readResult.increase();
            } else {
                entryData = defaultOneData();
            }
            Boolean writeSuccess = writeData(hashes, entryData);
            if (writeSuccess != null) {
                return writeSuccess;
            }
        }
    }


    private long[][] hash(long hash1, long hash2, int iterations, long size, int maxCount) {

        long index;
        long[][] indexes = new long[iterations][maxCount];
        long hash = hash1;
        for (int i = 0; i < iterations; i++) {

            index = ((hash & Long.MAX_VALUE) % size) * maxCount;

            for (int j = 0; j < maxCount; j++) {
                indexes[i][j] = index;
                index++;
            }

            if (i % 2 == 0) {
                hash += hash2;
            } else {
                hash += hash1;
            }
        }
        return indexes;
    }

    @Override
    public boolean contains(T object) {
        long[] hashes = hash(object);

        while (true) {
            ReadResult readResult = readData(hashes);
            if (readResult != null) {
                return readResult.success();
            }
        }
    }

    private ReadResult readData(long[] hashes) {
        if (size == 0) {
            readConfig();
        }

        int hashIterations = this.hashIterations;
        long size = this.size;
        int maxCount = this.maxBinaryBit;

        long[][] indexes = hash(hashes[0], hashes[1], hashIterations, size, maxCount);

        CommandBatchService executorService = new CommandBatchService(commandExecutor.getConnectionManager());
        addConfigCheck(hashIterations, size, executorService);
        RBitSetAsync bs = createBitSet(executorService);
        for (long[] index : indexes) {
            for (int j = 0; j < maxCount; j++) {
                bs.getAsync(index[j]);
            }
        }
        try {
            List<Boolean> result = (List<Boolean>) executorService.execute();

            List<Boolean> subResult = result.subList(1, result.size() - maxCount);

            int limit = subResult.size() / maxCount;
            List<List<Boolean>> splitList = new ArrayList<>(limit);
            Stream.iterate(0, n -> n + 1)
                    .limit(limit)
                    .forEach(i -> splitList.add(subResult.stream().skip((long) (i * maxCount)).limit(maxCount)
                            .collect(Collectors.toList())));

            boolean val;
            for (List<Boolean> resultList : splitList) {

                if (resultList == null) {
                    System.out.println("=================================0000000000000000000");
                }
                // 全部为false，则返回true, 标识没有值
                val = resultList.stream().noneMatch(item -> item);
                if (val) {
                    return initCheckResult(false, null);
                }
            }

            List<Boolean> resultList = splitList.get(0);
            Boolean[] resultBoolArray = new Boolean[resultList.size()];
            resultList.toArray(resultBoolArray);
            return initCheckResult(true, resultBoolArray);

        } catch (RedisException e) {
            if (!e.getMessage().contains("Bloom filter config has been changed")) {
                throw e;
            }
        }
        return null;
    }

    private Boolean writeData(long[] hashes, Boolean[] writeValue) {

        if (size == 0) {
            readConfig();
        }

        int hashIterations = this.hashIterations;
        long size = this.size;
        int maxCount = this.maxBinaryBit;

        long[][] indexes = hash(hashes[0], hashes[1], hashIterations, size, maxCount);

        CommandBatchService executorService = new CommandBatchService(commandExecutor.getConnectionManager());
        addConfigCheck(hashIterations, size, executorService);
        RBitSetAsync bs = createBitSet(executorService);
        for (long[] index : indexes) {
            for (int j = 0; j < maxCount; j++) {
                bs.setAsync(index[j], writeValue[j] == null ? false : writeValue[j]);
            }
        }
        try {
            List<Boolean> result = (List<Boolean>) executorService.execute();

            List<Boolean> subResult = result.subList(1, result.size() - maxCount);

            int limit = subResult.size() / maxCount;
            List<List<Boolean>> subList = new ArrayList<>(limit);
            Stream.iterate(0, n -> n + 1)
                    .limit(limit)
                    .forEach(i -> subList.add(subResult.stream().skip((long) (i * maxCount)).limit(maxCount)
                            .collect(Collectors.toList())));

            boolean val;
            for (List<Boolean> resultList : subList) {
                // 全部为false，则返回true
                val = resultList.stream().noneMatch(item -> item);
                if (val) {
                    return true;
                }
            }
            return false;
        } catch (RedisException e) {
            if (!e.getMessage().contains("Bloom filter config has been changed")) {
                throw e;
            }
        }
        return null;
    }

    @Override
    public boolean remove(T object) {

        long[] hashes = hash(object);

        while (true) {

            boolean exist = false;
            Boolean[] defaultArray = defaultOneData();
            ReadResult readResult = readData(hashes);

            if (readResult != null && readResult.success()) {
                exist = true;
                defaultArray = readResult.subtract();
            }

            if (!exist) {
                return false;
            }
            Boolean x = writeData(hashes, defaultArray);
            if (x != null) {
                return x;
            }
        }
    }

    private Boolean[] defaultOneData() {
        Boolean[] defaultArray = new Boolean[maxBinaryBit];
        for (int j = 0; j < maxBinaryBit; j++) {
            defaultArray[j] = Boolean.FALSE;
            if (j == maxBinaryBit - 1) {
                defaultArray[j] = Boolean.TRUE;
            }
        }
        return defaultArray;
    }

    protected RBitSetAsync createBitSet(CommandBatchService executorService) {
        return new RedissonBitSet(executorService, getName());
    }

    private void addConfigCheck(int hashIterations, long size, CommandBatchService executorService) {
        executorService.evalReadAsync(configName, codec, RedisCommands.EVAL_VOID,
                "local size = redis.call('hget', KEYS[1], 'size');" +
                        "local hashIterations = redis.call('hget', KEYS[1], 'hashIterations');" +
                        "assert(size == ARGV[1] and hashIterations == ARGV[2], 'Bloom filter config has been changed')",
                Arrays.asList(configName), size, hashIterations);
    }

    @Override
    public long count() {
        CommandBatchService executorService = new CommandBatchService(commandExecutor.getConnectionManager());
        RFuture<Map<String, String>> configFuture = executorService.readAsync(configName, StringCodec.INSTANCE,
                new RedisCommand<Map<Object, Object>>("HGETALL", new ObjectMapReplayDecoder()), configName);
        RBitSetAsync bs = createBitSet(executorService);
        RFuture<Long> cardinalityFuture = bs.cardinalityAsync();
        executorService.execute();

        readConfig(configFuture.getNow());

        return Math.round(-size / ((double) hashIterations) * Math.log(1 - cardinalityFuture.getNow() / ((double) size)));
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return commandExecutor.writeAsync(getName(), RedisCommands.DEL_OBJECTS, getName(), configName);
    }

    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        List<Object> keys = Arrays.asList(getName(), configName);
        return super.sizeInMemoryAsync(keys);
    }

    private void readConfig() {
        RFuture<Map<String, String>> future = commandExecutor.readAsync(configName, StringCodec.INSTANCE,
                new RedisCommand<Map<Object, Object>>("HGETALL", new ObjectMapReplayDecoder()), configName);
        Map<String, String> config = commandExecutor.get(future);

        readConfig(config);
    }

    private void readConfig(Map<String, String> config) {
        if (config.get("hashIterations") == null
                || config.get("size") == null) {
            throw new IllegalStateException("Bloom filter is not initialized!");
        }
        size = Long.valueOf(config.get("size"));
        hashIterations = Integer.valueOf(config.get("hashIterations"));
    }

    protected long getMaxSize() {
        return Integer.MAX_VALUE * 2L;
    }

    @Override
    public boolean tryInit(long expectedInsertions, double falseProbability) {
        if (falseProbability > 1) {
            throw new IllegalArgumentException("Counting Bloom filter false probability can't be greater than 1");
        }
        if (falseProbability < 0) {
            throw new IllegalArgumentException("Counting Bloom filter false probability can't be negative");
        }

        size = optimalNumOfBits(expectedInsertions, falseProbability);
        if (size == 0) {
            throw new IllegalArgumentException("Counting Bloom filter calculated size is " + size);
        }
        if (size * maxBinaryBit > getMaxSize()) {
            throw new IllegalArgumentException("Counting Bloom filter size can't be greater than " + getMaxSize() + ". But calculated size is " + size);
        }
        hashIterations = optimalNumOfHashFunctions(expectedInsertions, size);

        CommandBatchService executorService = new CommandBatchService(commandExecutor.getConnectionManager());
        executorService.evalReadAsync(configName, codec, RedisCommands.EVAL_VOID,
                "local size = redis.call('hget', KEYS[1], 'size');" +
                        "local hashIterations = redis.call('hget', KEYS[1], 'hashIterations');" +
                        "assert(size == false and hashIterations == false, 'Bloom filter config has been changed')",
                Arrays.asList(configName), size, hashIterations);
        executorService.writeAsync(configName, StringCodec.INSTANCE,
                new RedisCommand<Void>("HMSET", new VoidReplayConvertor()), configName,
                "size", size, "hashIterations", hashIterations,
                "expectedInsertions", expectedInsertions, "falseProbability", BigDecimal.valueOf(falseProbability).toPlainString());
        try {
            executorService.execute();
        } catch (RedisException e) {
            if (!e.getMessage().contains("Bloom filter config has been changed")) {
                throw e;
            }
            readConfig();
            return false;
        }

        return true;
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return redis.call('pexpire', KEYS[2], ARGV[1]); ",
                Arrays.asList(getName(), configName),
                timeUnit.toMillis(timeToLive));
    }

    @Override
    public RFuture<Boolean> expireAtAsync(long timestamp) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('pexpireat', KEYS[1], ARGV[1]); " +
                        "return redis.call('pexpireat', KEYS[2], ARGV[1]); ",
                Arrays.asList(getName(), configName),
                timestamp);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('persist', KEYS[1]); " +
                        "return redis.call('persist', KEYS[2]); ",
                Arrays.asList(getName(), configName));
    }

    @Override
    public long getExpectedInsertions() {
        Long result = commandExecutor.read(configName, LongCodec.INSTANCE, RedisCommands.HGET, configName, "expectedInsertions");
        return check(result);
    }

    @Override
    public double getFalseProbability() {
        Double result = commandExecutor.read(configName, DoubleCodec.INSTANCE, RedisCommands.HGET, configName, "falseProbability");
        return check(result);
    }

    @Override
    public long getSize() {
        Long result = commandExecutor.read(configName, LongCodec.INSTANCE, RedisCommands.HGET, configName, "size");
        return check(result);
    }

    @Override
    public int getHashIterations() {
        Integer result = commandExecutor.read(configName, IntegerCodec.INSTANCE, RedisCommands.HGET, configName, "hashIterations");
        return check(result);
    }

    private <V> V check(V result) {
        if (result == null) {
            throw new IllegalStateException("Counting Bloom filter is not initialized!");
        }
        return result;
    }


    private ReadResult initCheckResult(boolean success, Boolean[] resultList) {
        return new ReadResult() {

            @Override
            public boolean success() {
                return success;
            }

            @Override
            public Boolean[] increase() {

                if (resultList == null) {
                    return new Boolean[maxBinaryBit];
                }
                String binary = Arrays.stream(resultList)
                        .map(item -> Boolean.TRUE.equals(item) ? "1" : "0")
                        .collect(Collectors.joining());
                int resultInt = Integer.parseInt(binary, 2);

                int increaseInt = resultInt + 1;
                if (increaseInt > maxRepeat) {
                    return new Boolean[maxBinaryBit];
                }
                return addBinary2Array(resultList, defaultList);
            }

            @Override
            public Boolean[] subtract() {

                if (resultList == null) {
                    return new Boolean[maxBinaryBit];
                }
                String binary = Arrays.stream(resultList)
                        .map(item -> Boolean.TRUE.equals(item) ? "1" : "0")
                        .collect(Collectors.joining());
                int resultInt = Integer.parseInt(binary, 2);

                int subtractInt = resultInt - 1;
                if (subtractInt < 0) {
                    return new Boolean[maxBinaryBit];
                }
                return subBinary2Array(resultList, defaultList);
            }
        };
    }

    private Boolean[] addBinary2Array(Boolean[] a, Boolean[] b) {

        Boolean[] result = new Boolean[a.length];
        int s = 0;

        int i = a.length - 1;
        int j = b.length - 1;
        while (i >= 0 || j >= 0 || s == 1) {

            s += ((i >= 0) ? (a[i] ? 1 : 0) : 0);
            s += ((j >= 0) ? (b[j] ? 1 : 0) : 0);

            result[i] = s % 2 == 1;

            s /= 2;

            i--;
            j--;
        }

        return result;
    }

    private Boolean[] subBinary2Array(Boolean[] a, Boolean[] b) {

        Boolean[] result = new Boolean[a.length];

        int s = 0;
        int s1 = 0;
        int i = a.length - 1;
        int j = b.length - 1;
        while (i >= 0 || j >= 0) {

            s += ((i >= 0) ? (a[i] ? 1 : 0) : 0);
            s -= ((j >= 0) ? (b[j] ? 1 : 0) : 0);

            result[i] = (abs(s) % 2) == 1;

            if (s == -2) {
                s += 1;
            }
            if (s > 0) {
                s /= 2;
            }
            i--;
            j--;
        }

        return result;
    }

    interface ReadResult {

        /**
         * return success
         *
         * @return <code>true</code> find entry is success
         * <code>false</code> find entry is not found
         */
        boolean success();

        /**
         * array to store after adding the same element
         *
         * @return Array to store after adding the same element
         */
        Boolean[] increase();

        /**
         * array to store after deleting element
         *
         * @return array to store after deleting element
         */
        Boolean[] subtract();
    }

}
