package org.redisson;

import static java.lang.Math.abs;

public class BinaryOperationTest {

    public static void main(String[] args) {

        String binary = "";
        String a = "111";
        String b = "001";
        long startTime;
        long endTime;

        Boolean[] al = {true, false, false};
        Boolean[] bl = {false, false, true};

        startTime = System.nanoTime();
        binary = addBinary2(a, b);
        endTime = System.nanoTime();
        System.out.println("addBinary2(" + a + ", " + b + "):" + binary + " time:" + (endTime - startTime));

        startTime = System.nanoTime();
        binary = subBinary2(a, b);
        endTime = System.nanoTime();
        System.out.println("subBinary2(" + a + ", " + b + "):" + binary + " time:" + (endTime - startTime));

        startTime = System.nanoTime();
        int addInt = Integer.parseInt(al.toString(), 2);
        Boolean[] binary2List = addBinary2List(al, bl);
        endTime = System.nanoTime();
        System.out.println("subBinary2(" + al + ", " + bl + "):" + binary2List + " time:" + (endTime - startTime));
    }

    public static String addBinary3(String a, String b) {
        if (a == null || a.length() == 0)
            return b;
        if (b == null || b.length() == 0)
            return a;

        int pa = a.length() - 1;
        int pb = b.length() - 1;

        int flag = 0;
        StringBuilder sb = new StringBuilder();
        while (pa >= 0 || pb >= 0) {
            int va = 0;
            int vb = 0;

            if (pa >= 0) {
                va = a.charAt(pa) == '0' ? 0 : 1;
                pa--;
            }
            if (pb >= 0) {
                vb = b.charAt(pb) == '0' ? 0 : 1;
                pb--;
            }

            int sum = va + vb + flag;
            if (sum >= 2) {
                sb.append(String.valueOf(sum - 2));
                flag = 1;
            } else {
                flag = 0;
                sb.append(String.valueOf(sum));
            }
        }

        if (flag == 1) {
            sb.append("1");
        }

        return sb.reverse().toString();
    }

    public static String addBinary1(String a, String b) {
        StringBuilder sb = new StringBuilder();

        int i = a.length() - 1;
        int j = b.length() - 1;

        int carry = 0;

        while (i >= 0 || j >= 0) {
            int sum = 0;

            if (i >= 0 && a.charAt(i) == '1') {
                sum++;
            }

            if (j >= 0 && b.charAt(j) == '1') {
                sum++;
            }

            sum += carry;

            if (sum >= 2) {
                carry = 1;
            } else {
                carry = 0;
            }

            sb.insert(0, (char) ((sum % 2) + '0'));

            i--;
            j--;
        }

        if (carry == 1)
            sb.insert(0, '1');

        return sb.toString();
    }

    static String addBinary2(String a, String b) {

        StringBuilder result = new StringBuilder();

        int s = 0;

        int i = a.length() - 1;
        int j = b.length() - 1;
        while (i >= 0 || j >= 0 || s == 1) {

            s += ((i >= 0) ? a.charAt(i) - '0' : 0);
            s += ((j >= 0) ? b.charAt(j) - '0' : 0);

            result.insert(0, (char) (s % 2 + '0'));

            s /= 2;

            i--;
            j--;
        }

        return result.toString();
    }

    static String subBinary2(String a, String b) {

        StringBuilder result = new StringBuilder();

        int s = 0;
        int s1 = 0;
        int i = a.length() - 1;
        int j = b.length() - 1;
        while (i >= 0 || j >= 0) {

            s += ((i >= 0) ? a.charAt(i) - '0' : 0);
            s -= ((j >= 0) ? b.charAt(j) - '0' : 0);
            result.insert(0, (char) (abs(s) % 2 + '0'));

            if (s == -2) {
                s += 1;
            }
            if (s > 0) {
                s /= 2;
            }
            i--;
            j--;
        }

        return result.toString();
    }

    static Boolean[] addBinary2List(Boolean[] a, Boolean[] b) {

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

    static Boolean[] subBinary2Array(Boolean[] a, Boolean[] b) {

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
}
