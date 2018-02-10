package com.navinfo.qingqi.spark.ranking.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author miracle
 * @Date 2017/11/24 0024 15:04
 */
public class ListUtil<T> {

    /**
     * 说明：平均拆分list
     * size 拆分每组的个数
     */
    public List<List<T>> splitList(List<T> targe, int size) {
        List<List<T>> resultList = new ArrayList<List<T>>();String a = null;
        // 获取被拆分的数组个数
        int arrSize = targe.size() % size == 0 ? targe.size() / size : targe.size() / size + 1;
        for (int i = 0; i < arrSize; i++) {
            List<T> sub = new ArrayList<T>();
            // 把指定索引数据放入到list中
            int subSize = size * (i + 1) - 1;
            for (int j = i * size; j <= subSize; j++) {
                // 拆分最后一个子list时，避免获取元素越界
                if (j <= targe.size() - 1) {
                    sub.add(targe.get(j));
                }
            }
            resultList.add(sub);
        }
        return resultList;
    }

}
