package org.redisson;

import java.util.BitSet;

public class WhichChars {
    private BitSet used = new BitSet();

    public WhichChars(String str) {
        for (int i = 0; i < str.length(); i++)
            used.set(str.charAt(i));  // set bit for char
    }

    public String toString() {
        String desc = "[";
        int size = used.size();
        for (int i = 0; i < size; i++) {
            if (used.get(i))
                desc += (char) i;
        }
        return desc + "]";
    }

    public static void main(String args[]) {
        WhichChars w = new WhichChars("How do you do");
        System.out.println(w);
    }
}