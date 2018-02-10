package com.navinfo.qingqi.spark.ranking;


import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.navinfo.qingqi.spark.ranking.bean.CarRankingYesterdayEntity;
import com.navinfo.qingqi.spark.ranking.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.*;


/**
 * @author mc治华 + mc小帅
 * @Date 2017/11/21 0021 16:18
 */
public class OilRankingYesterday {

    private static Logger logger = LoggerFactory.getLogger(OilRankingYesterday.class);

    public static void main(String[] args) throws ClassNotFoundException {

        String date = DateUtil.formatDate(new Date(System.currentTimeMillis() - 86400000L));
        if(null != args  && args.length>0){
            date = DateUtil.formatDate(new Date(DateUtil.parseDate(args[0]).getTime() - 86400000L));
        }
        String mongoTableName = MongoUtil.getLastDayTableName(DateUtil.parseDate(date), MongoUtil.date_pattern2, MongoUtil.MONGO_BI);
        String mysqlTableName = "car_ranking_yesterday";

        String mysqlurl = PropertiesUtil.getProperties("mysql.url");
        String username = PropertiesUtil.getProperties("mysql.username");
        String password = PropertiesUtil.getProperties("mysql.password");

        Map<String,String> dbconfig = new HashMap<>();
        dbconfig.put("mysqlurl",mysqlurl);
        dbconfig.put("username",username);
        dbconfig.put("password",password);

        //创建SparkConf
        SparkConf sc = new SparkConf()
                //本地运行
//                .setMaster("local")
                        //应用名称
                .setAppName("OilRankingYesterday")
                        //mongodb input 连接
                .set("spark.mongodb.input.uri", PropertiesUtil.getProperties("spark.mongodb.input.uri"))
                .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner");

        //通过SparkConf 创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sc);

        //将数据库连接信息设为广播变量
        Broadcast<Map<String, String>> configCast = jsc.broadcast(dbconfig);

        //删除出要插入的数据，防止重复插入
        delRakingData(mysqlTableName, DateUtil.strTimeChangeLong(date + " 00:00:00"),mysqlurl,username,password);

        String mongoFilter = "'" + date + "'";
        //查询mongodb限制条件：里程大于十小于两千
        int minMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.minMeterInt"));
        int maxMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.maxMeterInt"));
        Map<String, String> readOverrides = new HashMap<String, String>(1);
        //拼接mongodb表名 bi_201711
        readOverrides.put("collection", mongoTableName);

        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        //从mongodb中查询上个月1号到最后一天,里程大于10，并且小于两千（meter_gps）的数据
        Document filter = Document.parse("{ $match: {date : " + mongoFilter + " ,\"data.meter_gps\":{$gte:" + minMeterInt + ",$lte:" + maxMeterInt + "}} }");
        //按照条件查询mongodb返回rdd
        JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(jsc, readConfig).withPipeline(Collections.singletonList(filter));

        //所有车辆缓存信息
        HashMap<String, HashMap<String, String>> carCache = CarCache.getCache();
        //如果当前没有车辆信息缓存，则查询MySQL，进行初始化
        if (null == carCache || carCache.size() == 0) {
            long carCacheStart = System.currentTimeMillis();
            CarCache.initCache();
            long carCacheEnd = System.currentTimeMillis();
            logger.info("Init car cache ,cost time : {}........", (carCacheEnd - carCacheStart));
        }

        Broadcast<HashMap<String, HashMap<String, String>>> mapB = jsc.broadcast(carCache);


        //处理从mongodb查询出来的数据
        final String finalDate = date;
        JavaRDD<CarRankingYesterdayEntity> javaRDD = javaMongoRDD.map(new Function<Document, CarRankingYesterdayEntity>() {
            @Override
            public CarRankingYesterdayEntity call(Document document) throws Exception {
                CarRankingYesterdayEntity carRankingYesterdayEntity = new CarRankingYesterdayEntity();
                String terminalId = String.valueOf(document.getLong("terminalId"));
                if (null != terminalId) {
                    //通过terminalId获取车辆信息
                    HashMap<String, String> carMap = mapB.getValue().get(terminalId);
                    if (null != carMap) {
                        String carModel = carMap.get("carModel");
                        String carNumber = carMap.get("carNumber");
                        String modelName = carMap.get("modelName");
                        String id = carMap.get("id");

                        double fuel = 0D;
                        double meterGps = 0D;
                        //判断是什么类型油耗排行计算：1是昨日油耗排行；2是上个月油耗排行；3是上周油耗排行
                        fuel = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("fuel")));
                        meterGps = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("meter_gps")));

                        //计算百公里油耗 ： fuel/meter_gps*100
                        double fuelConsumptionPerKM = fuel / meterGps * 100;

                        carRankingYesterdayEntity.setCar_id(id);
                        carRankingYesterdayEntity.setCar_num(carNumber);
                        carRankingYesterdayEntity.setStatis_date(finalDate);
                        carRankingYesterdayEntity.setMileage(meterGps);
                        carRankingYesterdayEntity.setOilwear(fuel);
                        carRankingYesterdayEntity.setOilwear_avg(fuelConsumptionPerKM);
                        carRankingYesterdayEntity.setCreate_time(DateUtil.format(DateUtil.time_pattern, new Date()));
                        carRankingYesterdayEntity.setStatis_timestamp(DateUtil.strTimeChangeLong(finalDate + " 00:00:00"));
                        carRankingYesterdayEntity.setCar_model(carModel);
                        carRankingYesterdayEntity.setModel_name(modelName);
                    }
                }
                return carRankingYesterdayEntity;
            }
            //过滤掉 mongodb数据中的terminalId在MySQL的car表中没有对应车辆信息的数据
        }).filter(new Function<CarRankingYesterdayEntity, Boolean>() {
            @Override
            public Boolean call(CarRankingYesterdayEntity carRankingYesterdayEntity) throws Exception {
                if (null != carRankingYesterdayEntity.getCar_id() && null != carRankingYesterdayEntity.getCar_num() && !"".equals(carRankingYesterdayEntity.getCar_num())) {
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
                return new Tuple2<String, CarRankingYesterdayEntity>(carRankingYesterdayEntity.getModel_name(), carRankingYesterdayEntity);
            }
        });


        JavaRDD<CarRankingYesterdayEntity> rankingRDD = javaPairRDD.groupByKey().mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Iterable<CarRankingYesterdayEntity>>>, CarRankingYesterdayEntity>() {
            @Override
            public List<CarRankingYesterdayEntity> call(Iterator<Tuple2<String, Iterable<CarRankingYesterdayEntity>>> tuple2Iterator) throws Exception {
                List<CarRankingYesterdayEntity> returnList = new ArrayList<CarRankingYesterdayEntity>();
                while (tuple2Iterator.hasNext()) {
                    List<CarRankingYesterdayEntity> groupList = new ArrayList<CarRankingYesterdayEntity>();
                    Tuple2<String, Iterable<CarRankingYesterdayEntity>> tuple2 = tuple2Iterator.next();
                    Iterator<CarRankingYesterdayEntity> iter = tuple2._2.iterator();
                    while (iter.hasNext()) {
                        CarRankingYesterdayEntity carRankingYesterdayEntity = iter.next();
                        if(carRankingYesterdayEntity.getOilwear_avg() > 10) {
                            groupList.add(carRankingYesterdayEntity);
                        }
                    }
                    //遍历某车型（Car_model）中车辆信息，并且计算该车型排行以及超过的百分比
                    if (groupList.size() > 0) {
                        //进行排序
                        Collections.sort(groupList, new Comparator() {
                            @Override
                            public int compare(Object a, Object b) {
                                int oneAvg = (int)(((CarRankingYesterdayEntity)a).getOilwear_avg()*10000);
                                int twoAvg = (int)(((CarRankingYesterdayEntity)b).getOilwear_avg()*10000);
                                return (oneAvg - twoAvg);
                            }
                        });
                        for (int y = 0; y < groupList.size(); y++) {
                            CarRankingYesterdayEntity carRankingYesterdayEntity = groupList.get(y);
                            int rank = y + 1;
                            carRankingYesterdayEntity.setRanking(rank);
                            carRankingYesterdayEntity.setPercentage(Math.floor((groupList.size() - rank + 0.0D) / groupList.size() * 100));
                            returnList.add(carRankingYesterdayEntity);
                        }
                    }
                }
                return returnList;
            }
        });

        batchInsert(rankingRDD.collect(), mysqlTableName, configCast.getValue().get("mysqlurl"), configCast.getValue().get("username"), configCast.getValue().get("password"));


        //删除上个月（30天）今天的油量排行数据(时间格式必须用：yyyy-MM-dd HH:mm:ss即time_pattern)
        delRakingData(mysqlTableName,DateUtil.returnSomeDay(date,30,DateUtil.time_pattern),configCast.getValue().get("mysqlurl"),configCast.getValue().get("username"),configCast.getValue().get("password"));
        }


    /**
     * 批量插入MySQL
     */
    public static void batchInsert(List<CarRankingYesterdayEntity> writeToMysql,String tableName,String mysqlurl,String username,String password){
        long writToMysqlStart = System.currentTimeMillis();
        //平均油耗小数点保留两位
        DecimalFormat df = new DecimalFormat("0.00");
        try {
            Connection conn = DriverManager.getConnection(mysqlurl, username, password);
            conn.setAutoCommit(false);
            String sql = "INSERT "+tableName+"(" +
                    "car_id," +
                    "car_num," +
                    "statis_date," +
                    "mileage," +
                    "oilwear," +
                    "oilwear_avg," +
                    "create_time," +
                    "statis_timestamp," +
                    "car_model" +
                    ",ranking," +
                    "percentage,model_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement prest = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

            ListUtil listUtil = new ListUtil();
            //把要插入MySQL的List按照一定数量分割，并且批量插入到MySQL中
            List<List<CarRankingYesterdayEntity>> splitWriteToMysql = listUtil.splitList(writeToMysql, 5000);

            for (int i = 0; i < splitWriteToMysql.size(); i++) {
                for (int x = 0; x < splitWriteToMysql.get(i).size(); x++) {
                    CarRankingYesterdayEntity carRankingYesterdayEntity = splitWriteToMysql.get(i).get(x);
                    prest.setString(1, carRankingYesterdayEntity.getCar_id());
                    prest.setString(2, carRankingYesterdayEntity.getCar_num());
                    prest.setString(3, carRankingYesterdayEntity.getStatis_date());
                    prest.setDouble(4, Double.parseDouble(df.format(carRankingYesterdayEntity.getMileage())));
                    prest.setDouble(5, Double.parseDouble(df.format(carRankingYesterdayEntity.getOilwear())));
                    prest.setDouble(6, Double.parseDouble(df.format(carRankingYesterdayEntity.getOilwear_avg())));
                    prest.setString(7, carRankingYesterdayEntity.getCreate_time());
                    prest.setLong(8, carRankingYesterdayEntity.getStatis_timestamp());
                    prest.setString(9, carRankingYesterdayEntity.getCar_model());
                    prest.setInt(10, carRankingYesterdayEntity.getRanking());
                    prest.setDouble(11, carRankingYesterdayEntity.getPercentage());
                    prest.setString(12, carRankingYesterdayEntity.getModel_name());
                    prest.addBatch();
                }
                prest.executeBatch();
                conn.commit();
            }
            long writToMysqlEnd = System.currentTimeMillis();
            logger.info("-----------writeToMysql size :{} , cost time : {}",writeToMysql.size(),(writToMysqlEnd - writToMysqlStart));
            prest.close();
            conn.close();
        } catch (Exception e) {
            logger.error("mysql insert error");
            e.printStackTrace();
        }

    }

    /**
     * 删除对应的油量排行数据
     * mysqlTableName 是要删除的表名（昨日：car_ranking_yesterday，上周：car_ranking_week，上个月：car_ranking_month）
     * statisTimestamp 是统计日期当天零点的时间戳十三位（昨日，上周：周一，上个月：月初一号）
     * @param mysqlTableName
     * @param statisTimestamp
     */
    public static void delRakingData(String mysqlTableName,Long statisTimestamp,String mysqlurl,String username,String password){
        try {
            Connection conn = DriverManager.getConnection(mysqlurl, username, password);
            String sql = "delete from "+mysqlTableName+" where statis_timestamp = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            //为占位符赋值
            pstmt.setLong(1, statisTimestamp);
            pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        }catch (Exception e){
            logger.error("MysqlUtil delRakingData error");
            e.printStackTrace();
        }

    }

}
