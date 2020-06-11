package org.redisson;

import java.util.Random;

/**
 * A Java implemetion of Cuckoo Filter
 */
public class CuckooFilter {

    static final int MAXIMUM_CAPACITY = 1 << 30;

    private final int MAX_NUM_KICKS = 500;
    private int capacity;
    private int size = 0;
    private Bucket[] buckets;
    private Random random;

    public CuckooFilter(int capacity) {
        capacity = tableSizeFor(capacity);
        this.capacity = capacity;
        buckets = new Bucket[capacity];
        random = new Random();
        for (int i = 0; i < capacity; i++) {
            buckets[i] = new Bucket();
        }
    }


    /**
     * insert an object into cuckoo filter
     *
     * @param o The object is to be inserted
     * @return false if filter consideredly full or insert an null object
     */
    public boolean insert(Object o) {
        if (o == null)
            return false;
        byte f = fingerprint(o);
        int i1 = hash(o);
        int i2 = i1 ^ hash(f);

        if (buckets[i1].insert(f) || buckets[i2].insert(f)) {
            size++;
            return true;
        }
        // must relocate existing items
        return relocateAndInsert(i1, i2, f);
    }

    /**
     * insert an object into cuckoo filter before checking whether the object is already inside
     *
     * @param o The object is to be inserted
     * @return false when filter consideredly full or the object is already inside
     */
    public boolean insertUnique(Object o) {
        if (o == null || contains(o))
            return false;
        return insert(o);
    }


    private boolean relocateAndInsert(int i1, int i2, byte f) {
        boolean flag = random.nextBoolean();
        int itemp = flag ? i1 : i2;
        for (int i = 0; i < MAX_NUM_KICKS; i++) {
            int position = random.nextInt(Bucket.BUCKET_SIZE);
            f = buckets[itemp].swap(position, f);
            itemp = itemp ^ hash(f);
            if (buckets[itemp].insert(f)) {
                size++;
                return true;
            }
        }
        return false;
    }


    /**
     * Returns <tt>true</tt> if this filter contains a fingerprint for the
     * object.
     *
     * @param o The object is to be tested
     * @return <tt>true</tt> if this map contains a fingerprint for the
     * object.
     */
    public boolean contains(Object o) {
        if (o == null)
            return false;
        byte f = fingerprint(o);
        int i1 = hash(o);
        int i2 = i1 ^ hash(f);
        return buckets[i1].contains(f) || buckets[i2].contains(f);
    }

    /**
     * delete object from cuckoo filter.Note that, to delete an item x safely, it must have been
     * previously inserted.
     *
     * @param o
     * @return <tt>true</tt> if this map contains a fingerprint for the
     * object.
     */
    public boolean delete(Object o) {
        if (o == null)
            return false;
        byte f = fingerprint(o);
        int i1 = hash(o);
        int i2 = i1 ^ hash(f);
        return buckets[i1].delete(f) || buckets[i2].delete(f);
    }

    /**
     * Returns the number of fingerprints in this map.
     *
     * @return the number of fingerprints in this map
     */
    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    private byte fingerprint(Object o) {
        int h = o.hashCode();
        h += ~(h << 15);
        h ^= (h >> 10);
        h += (h << 3);
        h ^= (h >> 6);
        h += ~(h << 11);
        h ^= (h >> 16);
        byte hash = (byte) h;
        if (hash == Bucket.NULL_FINGERPRINT)
            hash = 40;
        return hash;
    }

    public int hash(Object key) {
        int h = key.hashCode();
        h -= (h << 6);
        h ^= (h >> 17);
        h -= (h << 9);
        h ^= (h << 4);
        h -= (h << 3);
        h ^= (h << 10);
        h ^= (h >> 15);
        return h & (capacity - 1);
    }


    static final int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    static class Bucket {
        public static final int FINGERPINT_SIZE = 1;
        public static final int BUCKET_SIZE = 4;
        public static final byte NULL_FINGERPRINT = 0;

        private final byte[] fps = new byte[BUCKET_SIZE];

        public boolean insert(byte fingerprint) {
            for (int i = 0; i < fps.length; i++) {
                if (fps[i] == NULL_FINGERPRINT) {
                    fps[i] = fingerprint;
                    return true;
                }
            }
            return false;
        }


        public boolean delete(byte fingerprint) {
            for (int i = 0; i < fps.length; i++) {
                if (fps[i] == fingerprint) {
                    fps[i] = NULL_FINGERPRINT;
                    return true;
                }
            }
            return false;
        }

        public boolean contains(byte fingerprint) {
            for (int i = 0; i < fps.length; i++) {
                if (fps[i] == fingerprint)
                    return true;
            }
            return false;
        }

        public byte swap(int position, byte fingerprint) {
            byte tmpfg = fps[position];
            fps[position] = fingerprint;
            return tmpfg;
        }
    }

}