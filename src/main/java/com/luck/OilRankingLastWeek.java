package com.luck;

import com.luck.entity.CarRankingYesterdayEntity;
import com.luck.util.*;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * @Author miracle
 * @Date 2017/11/27 0027 10:18
 */
public class OilRankingLastWeek {

    private static Logger logger = LoggerFactory.getLogger(OilRankingLastWeek.class);

    public static void main(String[] args) {
        //mongodb查询条件
//        List<String> mongoCondition = MongoUtil.getLastWeekCondition(DateUtil.parseDate("2017-11-22"), MongoUtil.date_pattern1);
        List<String> mongoCondition = MongoUtil.getLastWeekCondition(new Date(), MongoUtil.date_pattern1);
        String beginDate = null;
        String endDate = null;
        if (null != mongoCondition && mongoCondition.size() == 2) {
            beginDate = mongoCondition.get(0);
            endDate = mongoCondition.get(1);
        }
        //判断开始结束日期是否存在
        if (null == beginDate || null == endDate) {
            logger.error("OilRankingLastWeek beginDate or endDate == null");
            return;
        }
        //过滤上周少于3条的车辆数据，不进行油量排行
        int carDataLimit = Integer.parseInt(PropertiesUtil.getProperties("carDataLimit.Week"));
        List<String> mongoTableName = null;
        String mysqlTableName = "car_ranking_week";
        mongoTableName = MongoUtil.getLastWeekTableName(new Date(), MongoUtil.date_pattern2, MongoUtil.MONGO_BI);
//        mongoTableName = MongoUtil.getLastWeekTableName(DateUtil.parseDate("2017-11-22"), MongoUtil.date_pattern2, MongoUtil.MONGO_BI);
        //判断mongodb表名是否为null
        if (null == mongoTableName){
            logger.error("OilRankingLastWeek mongoTableName == null");
            return;
        }

        //创建SparkConf
        SparkConf sc = new SparkConf()
                //本地运行
                .setMaster("local")
                        //应用名称
                .setAppName("OilRankingLastWeek")
                        //mongodb input 连接
                .set("spark.mongodb.input.uri", PropertiesUtil.getProperties("spark.mongodb.input.uri"))
                .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner");

        //通过SparkConf 创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<Document> javaMongoRDD = null;

        //查询范围不跨月
        if (mongoTableName.size() == 1) {
            javaMongoRDD = SparkUtil.getMongoDataLast(mongoTableName.get(0), jsc, beginDate, endDate);
        }
        //查询范围跨月
        else if (mongoTableName.size() == 2) {
            String monthEnd = DateUtil.lastDayOfCurrentMonth(beginDate);
            String monthStart = DateUtil.firstDayOfCurrentMonth(endDate);
            //计算上周跨月的开始日期到月底数据
            JavaMongoRDD<Document> javaMongoRDD1 = SparkUtil.getMongoDataLast(mongoTableName.get(0), jsc, beginDate, monthEnd);
            //计算上周跨月，下月月初到结束日期数据
            JavaMongoRDD<Document> javaMongoRDD2 = SparkUtil.getMongoDataLast(mongoTableName.get(0), jsc, monthStart, endDate);
            javaMongoRDD = javaMongoRDD1.union(javaMongoRDD2);
        }
        //判断是否有mongodb数据
        if (null == javaMongoRDD || javaMongoRDD.count() == 0) {
            logger.error("OilRankingYesterday javaMongoRDD is empty");
            return;
        }
        logger.info("OilRankingLastWeek javaMongoRDD size : {}",javaMongoRDD.count());

        //将查询的mongodb数据，按照车的terminalId分组，并且合并该terminalId车的里程，油耗信息
        JavaRDD<Document> javaRDD = SparkUtil.combineCarInfo(javaMongoRDD, carDataLimit, jsc);

        //待写入MySQL的昨日车辆油耗排行榜数据
        List<CarRankingYesterdayEntity> writeToMysql = SparkUtil.getListFromRDDLast(javaRDD, beginDate);
        //批量插入转换后的数据到mysql
        MysqlUtil.batchInsert(writeToMysql, mysqlTableName);
    }
}
