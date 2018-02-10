package com.navinfo.qingqi.spark.ranking;


import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.navinfo.qingqi.spark.ranking.bean.CarRankingYesterdayEntity;
import com.navinfo.qingqi.spark.ranking.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
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
 * @Date 2017/11/27 0027 10:18
 */
public class OilRankingLastMonth {

    private static Logger logger = LoggerFactory.getLogger(OilRankingLastMonth.class);

    public static void main(String[] args) {

        String date = DateUtil.formatDate(new Date());
        if (null != args && args.length > 0) {
            date = args[0];
        }
        String beginDate = null;
        String endDate = null;
        //mongo查询条件
        List<String> mongoCondition = MongoUtil.getLastMonthCondition(DateUtil.parseDate(date), MongoUtil.date_pattern1);
        if (null != mongoCondition && mongoCondition.size() == 2) {
            beginDate = mongoCondition.get(0);
            endDate = mongoCondition.get(1);
        }
        //判断开始结束日期是否存在
        if (null == beginDate || null == endDate) {
            logger.error("OilRankingLastWeek beginDate or endDate == null");
            return;
        }
        String mongoTableName = MongoUtil.getLastMonthTableName(DateUtil.parseDate(date), MongoUtil.date_pattern2, MongoUtil.MONGO_BI);

        String mysqlTableName = "car_ranking_month";
        //过滤上个月少于15条的车辆数据，不进行油量排行
        int carDataLimitMonth = Integer.parseInt(PropertiesUtil.getProperties("carDataLimit.Month"));

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
                .setAppName("OilRankingLastMonth")
                        //mongodb input 连接
                .set("spark.mongodb.input.uri", PropertiesUtil.getProperties("spark.mongodb.input.uri"))
                .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner");

        //通过SparkConf 创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sc);

        //将数据库连接信息设为广播变量
        Broadcast<Map<String, String>> configCast = jsc.broadcast(dbconfig);

        //spark获取mongodb中指定条件的数据
        JavaMongoRDD<Document> javaMongoRDD = SparkUtil.getMongoDataLast(mongoTableName, jsc, beginDate, endDate);

        //删除出要插入的数据，防止重复插入
        delRakingData(mysqlTableName, DateUtil.strTimeChangeLong(beginDate + " 00:00:00"),mysqlurl,username,password);

        //所有车辆缓存信息
        HashMap<String, HashMap<String, String>> carCache = CarCache.getCache();
        //如果当前没有车辆信息缓存，则查询MySQL，进行初始化
        if (null == carCache || carCache.size() == 0) {
            long carCacheStart = System.currentTimeMillis();
            CarCache.initCache();
            long carCacheEnd = System.currentTimeMillis();
            logger.info("Init car cache ,cost time : {}........", (carCacheEnd - carCacheStart));
        }

        //将车辆信息设为广播变量
        Broadcast<HashMap<String, HashMap<String, String>>> mapB = jsc.broadcast(carCache);

        //处理从mongodb查询出来的数据
        final String finalBeginDate = beginDate;
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

                        fuel = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("fuel")));
                        meterGps = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("meter_gps")));

                        //计算百公里油耗 ： fuel/meter_gps*100
                        double fuelConsumptionPerKM = fuel / meterGps * 100;
                        carRankingYesterdayEntity.setCar_id(id);
                        carRankingYesterdayEntity.setCar_num(carNumber);
                        carRankingYesterdayEntity.setStatis_date(finalBeginDate);
                        carRankingYesterdayEntity.setMileage(meterGps);
                        carRankingYesterdayEntity.setOilwear(fuel);
                        carRankingYesterdayEntity.setOilwear_avg(fuelConsumptionPerKM);
                        carRankingYesterdayEntity.setCreate_time(DateUtil.format(DateUtil.time_pattern, new Date()));
                        carRankingYesterdayEntity.setStatis_timestamp(DateUtil.strTimeChangeLong(finalBeginDate + " 00:00:00"));
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

        //按carID分组
        JavaPairRDD<String, CarRankingYesterdayEntity> terRDD = javaRDD.mapToPair(new PairFunction<CarRankingYesterdayEntity, String, CarRankingYesterdayEntity>() {
            @Override
            public Tuple2<String, CarRankingYesterdayEntity> call(CarRankingYesterdayEntity carRankingYesterdayEntity) throws Exception {
                return new Tuple2<String, CarRankingYesterdayEntity>(carRankingYesterdayEntity.getCar_id(), carRankingYesterdayEntity);
            }
        }).reduceByKey(new Function2<CarRankingYesterdayEntity, CarRankingYesterdayEntity, CarRankingYesterdayEntity>() {
            //对相同车辆的数据进行整合
            @Override
            public CarRankingYesterdayEntity call(CarRankingYesterdayEntity carRankingYesterdayEntity, CarRankingYesterdayEntity carRankingYesterdayEntity2) throws Exception {
                CarRankingYesterdayEntity crye = new CarRankingYesterdayEntity();
                crye.setCar_id(carRankingYesterdayEntity.getCar_id());
                crye.setCar_num(carRankingYesterdayEntity.getCar_num());
                crye.setStatis_date(carRankingYesterdayEntity.getStatis_date());
                crye.setMileage(carRankingYesterdayEntity.getMileage() + carRankingYesterdayEntity2.getMileage());
                crye.setOilwear(carRankingYesterdayEntity.getOilwear() + carRankingYesterdayEntity2.getOilwear());
                crye.setOilwear_avg(crye.getOilwear() / crye.getMileage() * 100);
                crye.setCreate_time(DateUtil.format(DateUtil.time_pattern, new Date()));
                crye.setStatis_timestamp(carRankingYesterdayEntity.getStatis_timestamp());
                crye.setCar_model(carRankingYesterdayEntity.getCar_model());
                crye.setModel_name(carRankingYesterdayEntity.getModel_name());
                crye.setVilidday(carRankingYesterdayEntity.getVilidday() + 1);
                return crye;
            }
        });


        //将算完百公里油耗的数据，按照车型（Car_model）进行转换为Tuple2
        JavaPairRDD<String, CarRankingYesterdayEntity> javaPairRDD = terRDD.mapToPair(new PairFunction<Tuple2<String, CarRankingYesterdayEntity>, String, CarRankingYesterdayEntity>() {
            @Override
            public Tuple2<String, CarRankingYesterdayEntity> call(Tuple2<String, CarRankingYesterdayEntity> stringCarRankingYesterdayEntityTuple2) throws Exception {
                return new Tuple2<String, CarRankingYesterdayEntity>(stringCarRankingYesterdayEntityTuple2._2.getModel_name(), stringCarRankingYesterdayEntityTuple2._2);
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
                        if (carRankingYesterdayEntity.getOilwear_avg() > 10) {
                            groupList.add(carRankingYesterdayEntity);
                        }
                    }
                    //进行排序
                    Collections.sort(groupList, new Comparator() {
                        @Override
                        public int compare(Object a, Object b) {
                            int oneAvg = (int) (((CarRankingYesterdayEntity) a).getOilwear_avg() * 10000);
                            int twoAvg = (int) (((CarRankingYesterdayEntity) b).getOilwear_avg() * 10000);
                            return (oneAvg - twoAvg);
                        }
                    });
                    //计算该车型排行以及超过的百分比
                    int rank = 0;
                    for (int y = 0; y < groupList.size(); y++) {
                        CarRankingYesterdayEntity carRankingYesterdayEntity = groupList.get(y);
                        if (carRankingYesterdayEntity.getVilidday() < carDataLimitMonth) {
                            continue;
                        }
                        rank++;
                        carRankingYesterdayEntity.setRanking(rank);
                        carRankingYesterdayEntity.setPercentage(Math.floor((groupList.size() - rank + 0.0D) / groupList.size() * 100));
                        returnList.add(carRankingYesterdayEntity);
                    }
                }
                return returnList;
            }
        });

        batchInsert(rankingRDD.collect(), mysqlTableName, configCast.getValue().get("mysqlurl"), configCast.getValue().get("username"), configCast.getValue().get("password"));

        //清理一年前的数据
        Calendar cal = Calendar.getInstance();
        cal.setTime(DateUtil.parseDate(beginDate));
        cal.add(Calendar.MONTH, -12);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        Date firstDayOfMonth = cal.getTime();
        delRakingData(mysqlTableName, firstDayOfMonth.getTime(),configCast.getValue().get("mysqlurl"),configCast.getValue().get("username"),configCast.getValue().get("password"));
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
            String createTime = DateUtil.format(DateUtil.time_pattern, new Date());
            for (int i = 0; i < splitWriteToMysql.size(); i++) {
                for (int x = 0; x < splitWriteToMysql.get(i).size(); x++) {
                    CarRankingYesterdayEntity carRankingYesterdayEntity = splitWriteToMysql.get(i).get(x);
                    prest.setString(1, carRankingYesterdayEntity.getCar_id());
                    prest.setString(2, carRankingYesterdayEntity.getCar_num());
                    prest.setString(3, carRankingYesterdayEntity.getStatis_date());
                    prest.setDouble(4, Double.parseDouble(df.format(carRankingYesterdayEntity.getMileage())));
                    prest.setDouble(5, Double.parseDouble(df.format(carRankingYesterdayEntity.getOilwear())));
                    prest.setDouble(6, Double.parseDouble(df.format(carRankingYesterdayEntity.getOilwear_avg())));
                    prest.setString(7, createTime);
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
