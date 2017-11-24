package com.luck;

import com.luck.entity.CarEntity;
import com.luck.entity.CarRankingYesterdayEntity;
import com.luck.util.*;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Date;

/**
 * @Author miracle
 * @Date 2017/11/21 0021 16:18
 */
public class OilRanking {
    private static Logger logger = LoggerFactory.getLogger(OilRanking.class);

    public static void main(String args[]) throws ClassNotFoundException {
        //查询昨日mongodb数据
        String date = "2017-11-12";
        //查询mongodb限制条件：里程大于十小于两千
        int minMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.minMeterInt"));
        int maxMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.maxMeterInt"));
        int mysqlBatchSize = Integer.parseInt(PropertiesUtil.getProperties("mysql.batch.size"));
        //待写入MySQL的昨日车辆油耗排行榜数据
        List<CarRankingYesterdayEntity> writeToMysql = new ArrayList<>();
        //平均油耗小数点保留两位
        DecimalFormat df = new DecimalFormat("0.00");
        //所有车辆缓存信息
        CarCache carCache = new CarCache();

        /////////////从MySQL中读取全量车信息(按条件过滤掉无效数据)，并封装成cache///////////////
        String driver = "com.mysql.jdbc.Driver";
        // URL指向要访问的数据库名game
        String url = PropertiesUtil.getProperties("mysql.url");
        // MySQL配置时的用户名
        String user = PropertiesUtil.getProperties("mysql.username");
        // MySQL配置时的密码
        String password = PropertiesUtil.getProperties("mysql.password");
        Class.forName(driver);
        try {
            try (
                    Connection conn = DriverManager.getConnection(url, user, password);
                    Statement statement = conn.createStatement();
                    //查询所有车信息，排除carModel为null的数据
                    ResultSet rs = statement.executeQuery("SELECT id, auto_terminal as autoTerminal,car_model as carModel,car_number as carNumber FROM car WHERE car_model is not null")
            ) {
                while (rs.next()) {
                    String id = rs.getString("id");
                    String autoTerminal = rs.getString("autoTerminal");
                    String carModel = rs.getString("carModel");
                    String carNumber = rs.getString("carNumber");
                    CarEntity carEntity = new CarEntity();
                    carEntity.setAutoTerminal(autoTerminal);
                    carEntity.setCarModel(carModel);
                    carEntity.setCarNumber(carNumber);
                    carEntity.setId(id);
                    carCache.addCar(carEntity);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException", e);
            e.printStackTrace();
        }
        logger.info("-----------get car from mysql size : {}",carCache.getCache().size());

        //创建SparkConf
        SparkConf sc = new SparkConf()
                //本地运行
                .setMaster("local")
                        //应用名称
                .setAppName("OilRanking")
                        //mongodb input 连接
                .set("spark.mongodb.input.uri", PropertiesUtil.getProperties("spark.mongodb.input.uri"))
                .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner");

        //通过SparkConf 创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sc);
        Map<String, String> readOverrides = new HashMap<String, String>();
        //拼接mongodb表名 bi_201711
        readOverrides.put("collection", "bi" + "_" + date.replace("-", "").substring(0, 6));

        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        //从mongodb中查询日期为date,里程大于10，并且小于两千（meter_gps）的数据
        Document filter = Document.parse("{ $match: {date :'" + date + "',\"data.meter_gps\":{$gte:" + minMeterInt + ",$lte:" + maxMeterInt + "}} }");
        //按照条件查询mongodb返回rdd
        JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(jsc, readConfig).withPipeline(Collections.singletonList(filter));
        logger.info("-----------get car fuel info from mongodb size :{}",javaMongoRDD.count());

        //处理从mongodb查询出来的数据
        JavaRDD<CarRankingYesterdayEntity> javaRDD = javaMongoRDD.map(new Function<Document, CarRankingYesterdayEntity>() {
            @Override
            public CarRankingYesterdayEntity call(Document document) throws Exception {
                CarRankingYesterdayEntity carRankingYesterdayEntity = new CarRankingYesterdayEntity();
                String terminalId = String.valueOf(document.getLong("terminalId"));
                //通过terminalId获取车辆信息
                HashMap<String, String> carMap = carCache.getCar(terminalId);
                String carModel = carMap.get("carModel");
                String carNumber = carMap.get("carNumber");
                String id = carMap.get("id");

                double fuel = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("fuel")));
                double meterGps = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("meter_gps")));
                //计算百公里油耗 ： fuel/meter_gps*100
                double fuelConsumptionPerKM = fuel / meterGps * 100;

                carRankingYesterdayEntity.setCar_id(id);
                carRankingYesterdayEntity.setCar_num(carNumber);
                carRankingYesterdayEntity.setStatis_date(date);
                carRankingYesterdayEntity.setMileage(meterGps);
                carRankingYesterdayEntity.setOilwear(fuel);
                carRankingYesterdayEntity.setOilwear_avg(fuelConsumptionPerKM);
                carRankingYesterdayEntity.setCreate_time(DateUtil.format(DateUtil.time_pattern, new Date()));
                carRankingYesterdayEntity.setStatis_timestamp(DateUtil.strTimeChangeLong(date + " 00:00:00"));
                carRankingYesterdayEntity.setCar_model(carModel);

                return carRankingYesterdayEntity;
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
                writeToMysql.add(carRankingYesterdayEntity);
            }
        }

        //批量插入转换后的数据到mysql
        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            String sql = "INSERT car_ranking_yesterday(" +
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
                    "percentage) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement prest = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

            ListUtil listUtil = new ListUtil();
            //把要插入MySQL的List按照一定数量分割，并且批量插入到MySQL中
            List<List<CarRankingYesterdayEntity>> splitWriteToMysql = listUtil.splitList(writeToMysql, mysqlBatchSize);
            logger.info("-----------writeToMysql size :{}",writeToMysql.size());

            for (int i = 0; i < splitWriteToMysql.size(); i++) {
                for (int x = 0; x < splitWriteToMysql.get(i).size(); x++) {
                    CarRankingYesterdayEntity carRankingYesterdayEntity = splitWriteToMysql.get(i).get(x);
                    prest.setString(1, carRankingYesterdayEntity.getCar_id());
                    prest.setString(2, carRankingYesterdayEntity.getCar_num());
                    prest.setString(3, carRankingYesterdayEntity.getStatis_date());
                    prest.setDouble(4, carRankingYesterdayEntity.getMileage());
                    prest.setDouble(5, carRankingYesterdayEntity.getOilwear());
                    prest.setDouble(6, Double.parseDouble(df.format(carRankingYesterdayEntity.getOilwear_avg())));
                    prest.setString(7, carRankingYesterdayEntity.getCreate_time());
                    prest.setLong(8, carRankingYesterdayEntity.getStatis_timestamp());
                    prest.setString(9, carRankingYesterdayEntity.getCar_model());
                    prest.setInt(10, carRankingYesterdayEntity.getRanking());
                    prest.setDouble(11, carRankingYesterdayEntity.getPercentage());
                    prest.addBatch();
                }
                prest.executeBatch();
                conn.commit();
            }
            conn.close();
        } catch (Exception e) {
            logger.error("mysql insert error");
            e.printStackTrace();
        }
    }
}
