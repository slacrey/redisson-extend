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

/**
 * Counting Bloom filter based on Highway 128-bit hash.
 *
 * @param <T> type of object
 * @author linfeng
 */
public class RedissonCountingBloomFilter<T> extends RedissonExpirable implements RCountingBloomFilter<T> {


    private int DEFAULT_MAX_REPEAT = 7;
    private volatile long size;
    // 最大计数
    private volatile int maxRepeat = DEFAULT_MAX_REPEAT;
    private volatile int maxBinaryBit = Integer.toBinaryString(maxRepeat).length();
    private volatile int hashIterations;

    private final CommandExecutor commandExecutor;
    private final String configName;

    protected RedissonCountingBloomFilter(CommandExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getName(), "config");
    }

    protected RedissonCountingBloomFilter(CommandExecutor commandExecutor, String name, int repeat) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getName(), "config");
        this.maxRepeat = repeat;
    }

    protected RedissonCountingBloomFilter(Codec codec, CommandExecutor commandExecutor, String name) {
        super(codec, commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getName(), "config");
    }

    protected RedissonCountingBloomFilter(Codec codec, CommandExecutor commandExecutor, String name, int repeat) {
        super(codec, commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getName(), "config");
        this.maxRepeat = repeat;
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

            Boolean[] prevArray = getPrevBooleanArray();
            Boolean[] defaultArray = getDefaultBooleanArray();

            CheckResult checkResult = readBit(hashes, defaultArray);

            if (checkResult != null && checkResult.success()) {

                defaultArray = checkResult.increase();
                prevArray = checkResult.result();
            }

            if (defaultArray == null) {
                return false;
            }
            Boolean x = writeBit(hashes, defaultArray, prevArray);
            if (x != null) {
                return x;
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
            CheckResult checkResult = readBit(hashes, getDefaultBooleanArray());
            if (checkResult != null) {
                return checkResult.success();
            }
        }
    }

    private CheckResult readBit(long[] hashes, Boolean[] defaultArray) {
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
            List<List<Boolean>> mglist = new ArrayList<>(limit);
            Stream.iterate(0, n -> n + 1).limit(limit).forEach(i -> {
                mglist.add(subResult.stream().skip(i * maxCount).limit(maxCount).collect(Collectors.toList()));
            });

            Boolean val;
            for (List<Boolean> resultList : mglist) {

                val = resultList.stream().filter(o -> o).findFirst().orElse(false);
                if (!val) {
                    return initCheckResult(false, Arrays.asList(defaultArray));
                }
            }

            return initCheckResult(true, mglist.get(0));

        } catch (RedisException e) {
            if (!e.getMessage().contains("Counting Bloom filter config has been changed")) {
                throw e;
            }
        }
        return null;
    }

    private Boolean writeBit(long[] hashes, Boolean[] writeValue, Boolean[] prevArray) {

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
                bs.setAsync(index[j], writeValue[j]);
            }
        }
        try {
            List<Boolean> result = (List<Boolean>) executorService.execute();

            List<Boolean> subResult = result.subList(1, result.size() - maxCount);

            int limit = subResult.size() / maxCount;
            List<List<Boolean>> subList = new ArrayList<>(limit);
            Stream.iterate(0, n -> n + 1).limit(limit).forEach(i -> {
                subList.add(subResult.stream().skip(i * maxCount).limit(maxCount).collect(Collectors.toList()));
            });

            for (List<Boolean> resultList : subList) {
                for (int i = 0; i < resultList.size(); i++) {
                    if (!prevArray[i].equals(resultList.get(i))) {
                        return false;
                    }
                }
            }

            return true;
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
            Boolean[] defaultArray = getDefaultBooleanArray();
            Boolean[] prevArray = getPrevBooleanArray();
            CheckResult checkResult = readBit(hashes, defaultArray);

            if (checkResult != null && checkResult.success()) {
                exist = true;
                defaultArray = checkResult.subtract();
                prevArray = checkResult.result();
            }

            if (!exist) {
                return false;
            }
            Boolean x = writeBit(hashes, defaultArray, prevArray);
            if (x != null) {
                return x;
            }
        }
    }

    private Boolean[] getPrevBooleanArray() {
        Boolean[] defaultArray = new Boolean[maxBinaryBit];
        for (int j = 0; j < maxBinaryBit; j++) {
            defaultArray[j] = Boolean.FALSE;
        }
        return defaultArray;
    }

    private Boolean[] getDefaultBooleanArray() {
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
                Arrays.<Object>asList(configName), size, hashIterations);
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
        List<Object> keys = Arrays.<Object>asList(getName(), configName);
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
        if (size > getMaxSize()) {
            throw new IllegalArgumentException("Counting Bloom filter size can't be greater than " + getMaxSize() + ". But calculated size is " + size);
        }
        hashIterations = optimalNumOfHashFunctions(expectedInsertions, size);

        CommandBatchService executorService = new CommandBatchService(commandExecutor.getConnectionManager());
        executorService.evalReadAsync(configName, codec, RedisCommands.EVAL_VOID,
                "local size = redis.call('hget', KEYS[1], 'size');" +
                        "local hashIterations = redis.call('hget', KEYS[1], 'hashIterations');" +
                        "assert(size == false and hashIterations == false, 'Bloom filter config has been changed')",
                Arrays.<Object>asList(configName), size, hashIterations);
        executorService.writeAsync(configName, StringCodec.INSTANCE,
                new RedisCommand<Void>("HMSET", new VoidReplayConvertor()), configName,
                "size", size, "hashIterations", hashIterations,
                "expectedInsertions", expectedInsertions, "falseProbability", BigDecimal.valueOf(falseProbability).toPlainString());
        try {
            executorService.execute();
        } catch (RedisException e) {
            if (!e.getMessage().contains("Counting Bloom filter config has been changed")) {
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
                Arrays.<Object>asList(getName(), configName),
                timeUnit.toMillis(timeToLive));
    }

    @Override
    public RFuture<Boolean> expireAtAsync(long timestamp) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('pexpireat', KEYS[1], ARGV[1]); " +
                        "return redis.call('pexpireat', KEYS[2], ARGV[1]); ",
                Arrays.<Object>asList(getName(), configName),
                timestamp);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('persist', KEYS[1]); " +
                        "return redis.call('persist', KEYS[2]); ",
                Arrays.<Object>asList(getName(), configName));
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


    private CheckResult initCheckResult(Boolean result, List<Boolean> resultList) {
        return new CheckResult() {

            private String binary = resultList.stream()
                    .map(item -> item ? "1" : "0").collect(Collectors.joining());
            private int resultInt = Integer.parseInt(binary, 2);

            @Override
            public Boolean success() {
                return result;
            }

            @Override
            public Boolean[] result() {

                return intConvert2BoolArray(resultInt);
            }

            @Override
            public Boolean[] increase() {

                int increaseInt = resultInt + 1;
                if (increaseInt > maxRepeat) {
                    return null;
                }
                return intConvert2BoolArray(increaseInt);
            }

            @Override
            public Boolean[] subtract() {

                int subtractInt = resultInt - 1;
                if (subtractInt < 0) {
                    return null;
                }
                return intConvert2BoolArray(subtractInt);
            }
        };
    }

    private Boolean[] intConvert2BoolArray(int tempInt) {

        String binary = Integer.toBinaryString(tempInt);
        int binaryBit = binary.length();
        Boolean[] subtractArray = new Boolean[maxBinaryBit];
        if (binaryBit < maxBinaryBit) {
            int n = 1;
            for (int i = maxBinaryBit-1; i >= 0; i--) {
                if (n <= binaryBit) {
                    subtractArray[i] = binary.charAt(binaryBit - n) == '1';
                    n++;
                } else {
                    subtractArray[i] = false;
                }
            }
        } else if (binaryBit == maxBinaryBit) {
            for (int i = 0; i < binary.length(); i++) {
                subtractArray[i] = binary.charAt(i) == '1';
            }
        } else {
            return null;
        }
        return subtractArray;
    }

    interface CheckResult {

        /**
         * return success
         *
         * @return <code>true</code> find entry is success
         * <code>false</code> find entry is not found
         */
        Boolean success();

        /**
         * return query element
         *
         * @return Array to store after adding the same element
         */
        Boolean[] result();

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
