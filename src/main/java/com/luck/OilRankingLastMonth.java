package com.luck;

import com.luck.entity.CarRankingYesterdayEntity;
import com.luck.util.*;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * @Author miracle
 * @Date 2017/11/27 0027 10:18
 */
public class OilRankingLastMonth {

    private static Logger logger = LoggerFactory.getLogger(OilRankingLastMonth.class);

    public static void main(String[] args) {

//        String beginDate = "2017-11-12";
//        String endDate = "2017-11-13";
        String beginDate = null;
        String endDate = null;
        //mongo查询条件
        List<String> mongoCondition = MongoUtil.getLastMonthCondition(new Date(), MongoUtil.date_pattern1);
        if (null != mongoCondition && mongoCondition.size() == 2) {
            beginDate = mongoCondition.get(0);
            endDate = mongoCondition.get(1);
        }
        //判断开始结束日期是否存在
        if (null == beginDate || null == endDate) {
            logger.error("OilRankingLastWeek beginDate or endDate == null");
            return;
        }
        String mongoTableName = MongoUtil.getLastMonthTableName(new Date(), MongoUtil.date_pattern2, MongoUtil.MONGO_BI);
//        String mongoTableName = "bi_201711";
        String mysqlTableName = "car_ranking_month";
        //过滤上个月少于15条的车辆数据，不进行油量排行
        int carDataLimitMonth = Integer.parseInt(PropertiesUtil.getProperties("carDataLimit.Month"));

        //创建SparkConf
        SparkConf sc = new SparkConf()
                //本地运行
                .setMaster("local")
                        //应用名称
                .setAppName("OilRankingLastMonth")
                        //mongodb input 连接
                .set("spark.mongodb.input.uri", PropertiesUtil.getProperties("spark.mongodb.input.uri"))
                .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner");

        //通过SparkConf 创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sc);
        //spark获取mongodb中指定条件的数据
        JavaMongoRDD<Document> javaMongoRDD = SparkUtil.getMongoDataLast(mongoTableName, jsc, beginDate, endDate);
        //判断是否有mongodb数据
        if (null == javaMongoRDD || javaMongoRDD.count() == 0) {
            logger.error("OilRankingYesterday javaMongoRDD is empty");
            return;
        }
        logger.info("OilRankingLastMonth javaMongoRDD size : {}", javaMongoRDD.count());
        //将查询的mongodb数据，按照车的terminalId分组，并且合并该terminalId车的里程，油耗信息
        JavaRDD<Document> javaRDD = SparkUtil.combineCarInfo(javaMongoRDD, carDataLimitMonth, jsc);

        //待写入MySQL的上个月车辆油耗排行榜数据
        List<CarRankingYesterdayEntity> writeToMysql = SparkUtil.getListFromRDDLast(javaRDD, beginDate);
        //批量插入转换后的数据到mysql
        MysqlUtil.batchInsert(writeToMysql, mysqlTableName);
    }
}
