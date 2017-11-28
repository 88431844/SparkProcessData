package com.luck;

import com.luck.entity.CarRankingYesterdayEntity;
import com.luck.util.*;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
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
        //mongo查询条件
        List<String> mongoCondition = MongoUtil.getLastMonthCondition(new Date(),MongoUtil.date_pattern1);
//        String beginDate = "2017-11-12";
//        String endDate = "2017-11-13";
        String beginDate = null;
        String endDate = null;
        if (null != mongoCondition && mongoCondition.size() == 2){
            beginDate = mongoCondition.get(0);
            endDate = mongoCondition.get(1);
        }
        //判断开始结束日期是否存在
        if (null == beginDate || null == endDate){
            return;
        }
        String mongoTableName = MongoUtil.getLastMonthTableName(new Date(), MongoUtil.date_pattern2, MongoUtil.MONGO_BI);
//        String mongoTableName = "bi_201711";
        String mysqlTableName = "car_ranking_month";
        //过滤上个月少于15条的车辆数据，不进行油量排行
        int carDataLimitMonth = Integer.parseInt(PropertiesUtil.getProperties("carDataLimit.Month"));
        //查询mongodb限制条件：里程大于十小于两千
        int minMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.minMeterInt"));
        int maxMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.maxMeterInt"));

        //所有车辆缓存信息
        HashMap<String, HashMap<String, String>> carCache = CarCache.getCache();
        //如果当前没有车辆信息缓存，则查询MySQL，进行初始化
        if (null == carCache || carCache.size() == 0){
            long carCacheStart = System.currentTimeMillis();
            CarCache.initCache();
            long carCacheEnd = System.currentTimeMillis();
            logger.info("Init car cache ,cost time : {}........",(carCacheEnd - carCacheStart));
        }

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
        Map<String, String> readOverrides = new HashMap<String, String>();
        //拼接mongodb表名 bi_201711
        readOverrides.put("collection", mongoTableName);

        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        //从mongodb中查询上个月1号到最后一天,里程大于10，并且小于两千（meter_gps）的数据
        Document filter = Document.parse("{ $match: {date : {\"$gte\":'" + beginDate + "',\"$lte\":'" + endDate + "'} ,\"data.meter_gps\":{$gte:" + minMeterInt + ",$lte:" + maxMeterInt + "}} }");
        //按照条件查询mongodb返回rdd
        long sparkLoadBiStart = System.currentTimeMillis();
        JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(jsc, readConfig).withPipeline(Collections.singletonList(filter));
        long sparkLoadBiEnd = System.currentTimeMillis();
        logger.info("-----------get car fuel info from mongodb bi table size :{} ,cost time : {}", javaMongoRDD.count(), (sparkLoadBiEnd - sparkLoadBiStart));

        //将从mongodb中按条件查询的数据，转化成Tuple2<Long, Document>结构
        JavaPairRDD<Long,Document> javaPairRDD = javaMongoRDD.mapToPair(new PairFunction<Document, Long, Document>() {
            @Override
            public Tuple2<Long, Document> call(Document document) throws Exception {
                return new Tuple2<Long, Document>(document.getLong("terminalId"),document);
            }
        });

        //将Tuple2<Long, Document>结构转换为Map<Long,List<Document>>结构
        Map<Long,List<Document>> listMap = new HashMap<>();
        Iterator it  = javaPairRDD.collect().iterator();
        while (it.hasNext()) {
            Tuple2<Long, Document> tuple2 = (Tuple2<Long, Document>) it.next();
            Long key = tuple2._1;
            Document value = tuple2._2;
            List<Document> documentList = listMap.get(key);
            if (null == documentList){
                documentList = new ArrayList<>();
            }
            documentList.add(value);
            listMap.put(key, documentList);
        }

        //排除车辆信息条数少于指定数量的数据
        List<Document> filerList = new ArrayList<>();
        for (Long key :listMap.keySet()){
            List<Document> list = listMap.get(key);
            //过滤掉每个月少于15条的记录，不进行油量排行
            if (null != list && list.size() >= carDataLimitMonth){
                filerList.addAll(list);
            }
        }

        //计算过滤后的每个车辆的这个月中的总油耗和总公里
        Map<Long,List<Document>> mapDocument = new HashMap<>();
        for (Document document : filerList){
            Long key = document.getLong("terminalId");
            List<Document> documentList = mapDocument.get(key);
            if (null == documentList){
                documentList = new ArrayList<>();
            }
            documentList.add(document);
            mapDocument.put(key,documentList);
        }

        //将过滤后的数据转换为 List<Document>结构
        List<Document> documentList = new ArrayList<>();
        for (Long terminalId : mapDocument.keySet()){
            List<Document> list = mapDocument.get(terminalId);
            double totalFuel = 0D;
            double totalMilage = 0D;
            for (Document document :list){
                try {
                    totalFuel += Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("fuel")));
                    totalMilage += Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("meter_gps")));
                }catch (Exception e){
                    logger.error("json convert error");
                    e.printStackTrace();
                }
            }
            //组装新的Document
            Document d = new Document();
            d.put("terminalId",terminalId);
            d.put("fuel",totalFuel);
            d.put("meter_gps",totalMilage);
            documentList.add(d);
        }
        //将List<Document>转化为JavaRDD<Document结构
       JavaRDD<Document> javaRDD = jsc.parallelize(documentList);

        //待写入MySQL的上个月车辆油耗排行榜数据
        List<CarRankingYesterdayEntity> writeToMysql = SparkUtil.getListFromRDDLastMonth(javaRDD, beginDate);
        //批量插入转换后的数据到mysql
        MysqlUtil.batchInsert(writeToMysql, mysqlTableName);
    }
}
