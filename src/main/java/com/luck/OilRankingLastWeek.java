package com.luck;

import com.luck.util.PropertiesUtil;

/**
 * @Author miracle
 * @Date 2017/11/27 0027 10:18
 */
public class OilRankingLastWeek {

    public static void main(String[] args) {
        String beginDate = "";
        String endDate = "";
        String mongoTableName = "";
        //查询mongodb限制条件：里程大于十小于两千
        int minMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.minMeterInt"));
        int maxMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.maxMeterInt"));
        int mysqlBatchSize = Integer.parseInt(PropertiesUtil.getProperties("mysql.batch.size"));
    }
}
