package com.luck;

import com.luck.entity.CarRankingYesterdayEntity;
import com.luck.util.*;
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
        if (null == mongoTableName) {
            logger.error("OilRankingYesterday mongoTableName == null");
            return;
        }

//        String date = "2017-11-12";
//        String mongoTableName = "bi_201711";
        String mysqlTableName = "car_ranking_yesterday";

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
        //spark获取mongodb中指定条件的数据
        JavaMongoRDD<Document> javaMongoRDD = SparkUtil.getMongoDataYesterday(mongoTableName, jsc, date);
        //判断是否有mongodb数据
        if (null == javaMongoRDD || javaMongoRDD.count() == 0) {
            logger.error("OilRankingYesterday javaMongoRDD is empty");
            return;
        }
        logger.info("OilRankingLastMonth javaMongoRDD size : {}", javaMongoRDD.count());
        //待写入MySQL的昨日车辆油耗排行榜数据
        List<CarRankingYesterdayEntity> writeToMysql = SparkUtil.getListFromRDDYesterday(javaMongoRDD, date);
        //删除出要插入的数据，防止重复插入
        MysqlUtil.delRakingData(mysqlTableName, DateUtil.strTimeChangeLong(date + " 00:00:00"));
        //批量插入转换后的数据到mysql
        MysqlUtil.batchInsert(writeToMysql, mysqlTableName);
        //删除上个月（30天）今天的油量排行数据(时间格式必须用：yyyy-MM-dd HH:mm:ss即time_pattern)
        MysqlUtil.delRakingData(mysqlTableName,DateUtil.returnSomeDay(date,30,DateUtil.time_pattern));
    }
}
