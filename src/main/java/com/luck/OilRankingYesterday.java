package com.luck;

import com.luck.entity.CarRankingYesterdayEntity;
import com.luck.util.*;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.Date;

/**
 * @Author miracle
 * @Date 2017/11/21 0021 16:18
 */
public class OilRankingYesterday {
    private static Logger logger = LoggerFactory.getLogger(OilRankingYesterday.class);


    public static void main(String args[]) throws ClassNotFoundException {
        //查询昨日mongodb数据
        String date = MongoUtil.getLastDayCondition(new Date(), MongoUtil.date_pattern1);
        String mongoTableName = MongoUtil.getLastDayTableName(new Date(), MongoUtil.date_pattern2, MongoUtil.MONGO_BI);
//        String date = "2017-11-12";
//        String mongoTableName = "bi_201711";

        String mysqlTableName = "car_ranking_yesterday";
        //查询mongodb限制条件：里程大于十小于两千
        int minMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.minMeterInt"));
        int maxMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.maxMeterInt"));

        //所有车辆缓存信息
        HashMap<String, HashMap<String, String>> carCache = CarCache.getCache();
        //如果当前没有车辆信息缓存，则查询MySQL，进行初始化
        if (null == carCache || carCache.size() == 0){
            CarCache.initCache();
        }

        //创建SparkConf
        SparkConf sc = new SparkConf()
                //本地运行
                .setMaster("local")
                        //应用名称
                .setAppName("OilRankingYesterday")
                        //mongodb input 连接
                .set("spark.mongodb.input.uri", PropertiesUtil.getProperties("spark.mongodb.input.uri"))
                .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner");

        //通过SparkConf 创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sc);
        Map<String, String> readOverrides = new HashMap<String, String>();
        //拼接mongodb表名 bi_201711
        readOverrides.put("collection", mongoTableName);

        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        //从mongodb中查询日期为date,里程大于10，并且小于两千（meter_gps）的数据
        Document filter = Document.parse("{ $match: {date :'" + date + "',\"data.meter_gps\":{$gte:" + minMeterInt + ",$lte:" + maxMeterInt + "}} }");
        //按照条件查询mongodb返回rdd
        long sparkLoadBiStart = System.currentTimeMillis();
        JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(jsc, readConfig).withPipeline(Collections.singletonList(filter));
        long sparkLoadBiEnd = System.currentTimeMillis();
        logger.info("-----------get car fuel info from mongodb bi table size :{} ,cost time : {}",javaMongoRDD.count(),(sparkLoadBiEnd - sparkLoadBiStart));

        //待写入MySQL的昨日车辆油耗排行榜数据
        List<CarRankingYesterdayEntity> writeToMysql = SparkUtil.getListFromRDDYesterday(javaMongoRDD,date);
        //批量插入转换后的数据到mysql
        MysqlUtil.batchInsert(writeToMysql,mysqlTableName);
    }
}
