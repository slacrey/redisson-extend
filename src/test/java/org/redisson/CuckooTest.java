package org.redisson;

import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;

import java.nio.charset.Charset;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.math.LongMath.mod;
import static java.util.Objects.hash;

public class CuckooTest {

    public static void main(String[] args) {
        final long hash64 = hash64("h");
        final int hash1 = (int) hash64;
        final int hash2 = (int) (hash64 >>> 32);

        final int fingerprint = fingerPrint(hash2, 100);

        final long index1 = index(hash1, 10L);
        final long index2 = altIndex(index1, fingerprint, 10L);
        final long index3 = altIndex(index2, fingerprint, 10L);

        System.out.println(hash1);
        System.out.println(index1);
        System.out.println(index2);
        System.out.println(index3);


    }


    static long index(int hash, long m) {
        return mod(hash, m);
    }

    static private long hashLong(long i) {
        return Hashing.murmur3_128().hashLong(i).asLong();
    }

    static long altIndex(long index, int fingerprint, long m) {
        checkArgument(0L <= index, "index must be a positive!");
        checkArgument((0L <= m) && (0L == (m & 0x1L)), "m must be a positive even number!");
        return mod(protectedSum(index, paritySign(index) * odd(hashLong(fingerprint)), m), m);
    }

    static private long paritySign(long i) {
        return ((i & 0x01L) * -2L) + 1L;
    }

    static private long odd(long i) {
        return i | 0x01L;
    }

    static private long protectedSum(long index, long offset, long mod) {
        return canSum(index, offset) ? index + offset : protectedSum(index - mod, offset, mod);
    }

    static private boolean canSum(long a, long b) {
        return (a ^ b) < 0 || (a ^ (a + b)) >= 0;
    }


    static long hash64(String object) {

        return Hashing.murmur3_128().hashObject(object, Funnels.stringFunnel(UTF_8)).asLong();
    }

    static int fingerPrint(int hash, int unit) {

        int mask = (0x80000000 >> (unit - 1)) >>> (Long.SIZE - unit);

        for (int bit = 0; (bit + unit) <= Long.SIZE; bit += unit) {
            int ret = (hash >> bit) & mask;
            if (0 != ret) {
                return ret;
            }
        }
        return 0x1;
    }


}
