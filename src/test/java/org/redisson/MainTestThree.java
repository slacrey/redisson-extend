package org.redisson;
  
import java.util.BitSet;  
  
public class MainTestThree {  
  
    /**  
     * @param args  
     */  
    public static void main(String[] args) {  
        BitSet bm=new BitSet();  
        System.out.println(bm.isEmpty()+"--"+bm.size());  
        bm.set(0);  
        System.out.println(bm.isEmpty()+"--"+bm.size());  
        bm.set(1);  
        System.out.println(bm.isEmpty()+"--"+bm.size());  
        System.out.println(bm.get(65));  
        System.out.println(bm.isEmpty()+"--"+bm.size());  
        bm.set(65);  
        System.out.println(bm.isEmpty()+"--"+bm.size());  
    }  
  
}