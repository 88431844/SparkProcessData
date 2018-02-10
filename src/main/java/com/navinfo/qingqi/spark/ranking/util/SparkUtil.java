package com.navinfo.qingqi.spark.ranking.util;


import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.navinfo.qingqi.spark.ranking.bean.CarRankingYesterdayEntity;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * @author miracle
 * @Date 2017/11/28 0028 14:16
 */
public class SparkUtil implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(SparkUtil.class);

    /**
     * 计算油耗排行
     * 入参是JavaRDD 返回处理后的 List<CarRankingYesterdayEntity>
     *
     * @param rdd
     * @param statisDate
     * @param type       1是昨日油耗排行；2是上个月或者上周油耗排行
     * @return
     */
    private static List<CarRankingYesterdayEntity> getListFromRDD(JavaRDD rdd, String statisDate, String type) {
        List<CarRankingYesterdayEntity> retList = new ArrayList<>();
        //所有车辆缓存信息
        HashMap<String, HashMap<String, String>> carCache = CarCache.getCache();
        //如果当前没有车辆信息缓存，则查询MySQL，进行初始化
        if (null == carCache || carCache.size() == 0) {
            long carCacheStart = System.currentTimeMillis();
            CarCache.initCache();
            long carCacheEnd = System.currentTimeMillis();
            logger.info("Init car cache ,cost time : {}........", (carCacheEnd - carCacheStart));
        }
        //处理从mongodb查询出来的数据
        JavaRDD<CarRankingYesterdayEntity> javaRDD = rdd.map(new Function<Document, CarRankingYesterdayEntity>() {
            @Override
            public CarRankingYesterdayEntity call(Document document) throws Exception {
                CarRankingYesterdayEntity carRankingYesterdayEntity = new CarRankingYesterdayEntity();
                String terminalId = String.valueOf(document.getLong("terminalId"));
                if (null != terminalId) {
                    //通过terminalId获取车辆信息
                    HashMap<String, String> carMap = CarCache.getCar(terminalId);
                    if (null != carMap) {
                        String carModel = carMap.get("carModel");
                        String carNumber = carMap.get("carNumber");
                        String id = carMap.get("id");

                        double fuel = 0D;
                        double meterGps = 0D;
                        //判断是什么类型油耗排行计算：1是昨日油耗排行；2是上个月油耗排行；3是上周油耗排行
                        if ("1".equals(type)) {
                            fuel = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("fuel")));
                            meterGps = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("meter_gps")));
                        } else if ("2".equals(type)) {
                            fuel = document.getDouble("fuel");
                            meterGps = document.getDouble("meter_gps");
                        }
                        //计算百公里油耗 ： fuel/meter_gps*100
                        double fuelConsumptionPerKM = fuel / meterGps * 100;

                        carRankingYesterdayEntity.setCar_id(id);
                        carRankingYesterdayEntity.setCar_num(carNumber);
                        carRankingYesterdayEntity.setStatis_date(statisDate);
                        carRankingYesterdayEntity.setMileage(meterGps);
                        carRankingYesterdayEntity.setOilwear(fuel);
                        carRankingYesterdayEntity.setOilwear_avg(fuelConsumptionPerKM);
                        carRankingYesterdayEntity.setCreate_time(DateUtil.format(DateUtil.time_pattern, new Date()));
                        carRankingYesterdayEntity.setStatis_timestamp(DateUtil.strTimeChangeLong(statisDate + " 00:00:00"));
                        carRankingYesterdayEntity.setCar_model(carModel);
                    }
                }
                return carRankingYesterdayEntity;
            }
            //过滤掉 mongodb数据中的terminalId在MySQL的car表中没有对应车辆信息的数据
        }).filter(new Function<CarRankingYesterdayEntity, Boolean>() {
            @Override
            public Boolean call(CarRankingYesterdayEntity carRankingYesterdayEntity) throws Exception {
                if (null != carRankingYesterdayEntity.getCar_id()) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        //将算完百公里油耗的数据，按照车型（Car_model）进行转换为Tuple2
        JavaPairRDD<String, CarRankingYesterdayEntity> javaPairRDD = javaRDD.mapToPair(new PairFunction<CarRankingYesterdayEntity, String, CarRankingYesterdayEntity>() {
            @Override
            public Tuple2<String, CarRankingYesterdayEntity> call(CarRankingYesterdayEntity carRankingYesterdayEntity) throws Exception {
                return new Tuple2<String, CarRankingYesterdayEntity>(carRankingYesterdayEntity.getCar_model(), carRankingYesterdayEntity);
            }
        });

        //将Tuple2结构转化为Map<String, List<CarRankingYesterdayEntity>> 结构
        Map<String, List<CarRankingYesterdayEntity>> hashMap = new HashMap<>();
        Iterator it = javaPairRDD.sortByKey().collect().iterator();
        while (it.hasNext()) {
            Tuple2<String, CarRankingYesterdayEntity> tuple2 = (Tuple2<String, CarRankingYesterdayEntity>) it.next();
            String key = tuple2._1;
            CarRankingYesterdayEntity carRankingYesterdayEntity = tuple2._2;
            List<CarRankingYesterdayEntity> valueList = hashMap.get(key);
            if (null == valueList) {
                valueList = new ArrayList<>();
            }
            valueList.add(carRankingYesterdayEntity);
            hashMap.put(key, valueList);

        }

        //遍历 Map<String, List<CarRankingYesterdayEntity>> 并按照车型进行油耗排名以及百分比计算
        for (Map.Entry<String, List<CarRankingYesterdayEntity>> entry : hashMap.entrySet()) {
            List<CarRankingYesterdayEntity> clist = entry.getValue();
            int clistsize = clist.size();
            //遍历某车型（Car_model）中车辆信息，并且计算该车型排行以及超过的百分比
            for (int y = 0; y < clistsize; y++) {
                CarRankingYesterdayEntity carRankingYesterdayEntity = clist.get(y);
                int rank = y + 1;
                carRankingYesterdayEntity.setRanking(rank);
                carRankingYesterdayEntity.setPercentage(Math.floor((clistsize - rank + 0.0D) / clistsize * 100));
                retList.add(carRankingYesterdayEntity);
            }
        }
        return retList;
    }

    /**
     * 计算昨日里程油耗排行
     *
     * @param rdd
     * @param statisDate
     * @return
     */
    public static List<CarRankingYesterdayEntity> getListFromRDDYesterday(JavaRDD rdd, String statisDate) {
        return getListFromRDD(rdd, statisDate, "1");
    }

    /**
     * 计算上个月或者上周油耗排行
     *
     * @param rdd
     * @param statisDate
     * @return
     */
    public static List<CarRankingYesterdayEntity> getListFromRDDLast(JavaRDD rdd, String statisDate) {
        return getListFromRDD(rdd, statisDate, "2");
    }

    /**
     * spark通过条件，获取mongodb中数据
     *
     * @return
     */
    private static JavaMongoRDD<Document> getMongoData(String mongoTableName, JavaSparkContext jsc, String mongoFilter) {
        //查询mongodb限制条件：里程大于十小于两千
        int minMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.minMeterInt"));
        int maxMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.maxMeterInt"));
        Map<String, String> readOverrides = new HashMap<String, String>();
        //拼接mongodb表名 bi_201711
        readOverrides.put("collection", mongoTableName);

        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        //从mongodb中查询上个月1号到最后一天,里程大于10，并且小于两千（meter_gps）的数据
        Document filter = Document.parse("{ $match: {date : " + mongoFilter + " ,\"data.meter_gps\":{$gte:" + minMeterInt + ",$lte:" + maxMeterInt + "}} }");
        //按照条件查询mongodb返回rdd
        long sparkLoadBiStart = System.currentTimeMillis();
        JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(jsc, readConfig).withPipeline(Collections.singletonList(filter));
        long sparkLoadBiEnd = System.currentTimeMillis();

        return javaMongoRDD;
    }

    /**
     * spark通过条件，获取mongodb中数据 开始和结束时间段
     *
     * @param mongoTableName
     * @param jsc
     * @param beginDate
     * @param endDate
     * @return
     */
    public static JavaMongoRDD<Document> getMongoDataLast(String mongoTableName, JavaSparkContext jsc, String beginDate, String endDate) {
        String mongoFilter = "{\"$gte\":'" + beginDate + "',\"$lte\":'" + endDate + "'}";
        return getMongoData(mongoTableName, jsc, mongoFilter);
    }

    /**
     * spark通过条件，获取mongodb中昨天数据
     *
     * @param mongoTableName
     * @param jsc
     * @param date
     * @return
     */
    public static JavaMongoRDD<Document> getMongoDataYesterday(String mongoTableName, JavaSparkContext jsc, String date) {
        String mongoFilter = "'" + date + "'";
        return getMongoData(mongoTableName, jsc, mongoFilter);
    }

    /**
     * 将查询的mongodb数据，按照车的terminalId分组;
     * 并且合并该terminalId车的里程，油耗信息;
     * 最后过滤掉 该terminalId车，少于3条（上周油耗排行），或者15条（上个月油耗排行）的数据
     *
     * @param javaMongoRDD
     * @param carDataLimit
     * @param jsc
     * @return
     */
    public static JavaRDD<Document> combineCarInfo(JavaRDD<Document> javaMongoRDD, int carDataLimit, JavaSparkContext jsc) {
        //将从mongodb中按条件查询的数据，转化成Tuple2<Long, Document>结构
        JavaPairRDD<Long, Document> javaPairRDD = javaMongoRDD.mapToPair(new PairFunction<Document, Long, Document>() {
            @Override
            public Tuple2<Long, Document> call(Document document) throws Exception {
                return new Tuple2<Long, Document>(document.getLong("terminalId"), document);
            }
        });

        //将Tuple2<Long, Document>结构转换为Map<Long,List<Document>>结构
        Map<Long, List<Document>> listMap = new HashMap<>();
        Iterator it = javaPairRDD.collect().iterator();
        while (it.hasNext()) {
            Tuple2<Long, Document> tuple2 = (Tuple2<Long, Document>) it.next();
            Long key = tuple2._1;
            Document value = tuple2._2;
            List<Document> documentList = listMap.get(key);
            if (null == documentList) {
                documentList = new ArrayList<>();
            }
            documentList.add(value);
            listMap.put(key, documentList);
        }

        //排除车辆信息条数少于指定数量的数据
        List<Document> filerList = new ArrayList<>();
        for (Long key : listMap.keySet()) {
            List<Document> list = listMap.get(key);
            //过滤掉每个月少于15条的记录，不进行油量排行
            if (null != list && list.size() >= carDataLimit) {
                filerList.addAll(list);
            }
        }

        //计算过滤后的每个车辆的这个月中的总油耗和总公里
        Map<Long, List<Document>> mapDocument = new HashMap<>();
        for (Document document : filerList) {
            Long key = document.getLong("terminalId");
            List<Document> documentList = mapDocument.get(key);
            if (null == documentList) {
                documentList = new ArrayList<>();
            }
            documentList.add(document);
            mapDocument.put(key, documentList);
        }

        //将过滤后的数据转换为 List<Document>结构
        List<Document> documentList = new ArrayList<>();
        for (Long terminalId : mapDocument.keySet()) {
            List<Document> list = mapDocument.get(terminalId);
            double totalFuel = 0D;
            double totalMilage = 0D;
            for (Document document : list) {
                try {
                    totalFuel += Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("fuel")));
                    totalMilage += Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("meter_gps")));
                } catch (Exception e) {
                    logger.error("json convert error");
                    e.printStackTrace();
                }
            }
            //组装新的Document
            Document d = new Document();
            d.put("terminalId", terminalId);
            d.put("fuel", totalFuel);
            d.put("meter_gps", totalMilage);
            documentList.add(d);
        }
        //将List<Document>转化为JavaRDD<Document结构
        return jsc.parallelize(documentList);
    }


    public static String dealCarNum(String content){
        if(content == null || content.length()<6){
            return "*******";
        }
        String starStr = "";
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < (content.length() - 3); i++) {
            stringBuilder.append("*");
        }
        starStr = stringBuilder.toString();
        return content.substring(0, 2) + starStr
                + content.substring(content.length() - 1, content.length());
    }
}
