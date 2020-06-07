package org.redisson;

/**
 * @author linfeng
 * @date 2020-06-06
 **/
public class ConvertBinaryTest {

    private static int maxBinaryBit = 4;

    public static void main(String[] args) {


        System.out.println(intConvert2BoolArray(7).toString());
        System.out.println(intConvert2BoolArray(8).toString());
        System.out.println(intConvert2BoolArray(9).toString());
    }

    private static Boolean[] intConvert2BoolArray(int tempInt) {

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

}
