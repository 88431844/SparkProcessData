package com.luck;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author miracle
 * @Date 2017/11/27 0027 15:55
 */
public class Test {
    public static void main(String[] args) {
        HashMap<String,Map<String,String>> hashMap = new HashMap<>();

        Map<String,String> map1 = new HashMap<>();
        map1.put("1","111");
        map1.put("1","222");

        Map<String,String> map2 = new HashMap<>();
        map2.put("2","111");
        map2.put("2","222");

        hashMap.put("1",map1);
        hashMap.put("1",map2);

        System.out.println(hashMap.get("1").size());
    }
}
