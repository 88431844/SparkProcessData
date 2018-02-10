package com.navinfo.qingqi.spark.ranking;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.navinfo.qingqi.spark.ranking.bean.OilActivityCarMappingEntity;
import com.navinfo.qingqi.spark.ranking.bean.OilWearActivityEntity;
import com.navinfo.qingqi.spark.ranking.util.*;
import org.apache.commons.lang3.StringUtils;
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
import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Date;


/**
 * 节油大赛spark定时任务
 *
 * @author gzh
 * @Date 2018年1月25日 16:37:30
 */
public class OilGame {

    private static Logger logger = LoggerFactory.getLogger(OilGame.class);

    /**
     * 活动未开始
     */
    private final static int ACTIVITY_NOT_BEGIN = 1;
    /**
     * 活动进行中
     */
    private final static int ACTIVITYING = 2;
    /**
     * 活动已结束
     */
    private final static int ACTIVITY_DONE = 3;
    /**
     * 活动无效
     */
    private final static int ACTIVITY_INVALID = 4;

    /**
     * 入参日期大于当前日期
     */
    private final static int BIG_THAN_NOW = 1;
    /**
     * 入参日期小于当前日期
     */
    private final static int SMALL_THAN_NOW = 2;
    /**
     * 入参日期等于当前日期
     */
    private final static int SAME_AS_NOW = 3;
    /**
     * 入参日期小于当前日期，且小于1天或1天以上
     */
    private final static int SMALL_THAN_NOW_MORE_ONE = 4;

    /**
     * 活动表
     */
    private final static String ACTIVITY_TABLENAME = "oil_wear_activity";
    /**
     * 参与活动车辆表
     */
    private final static String ACTIVITY_CAR_TABLENAME = "oil_activity_car_mapping";

    /**
     * 有效 活动车辆
     */
    private final static int ACTIVITY_CAR_FLAG_NOT_END = 0;
    /**
     * 被终止 活动车辆
     */
    private final static int ACTIVITY_CAR_FLAG_END = 1;
//    /**
//     * 活动列表中第一个活动（正常符合条件的活动只能有一条记录）
//     */
//    private final static int FIRST_ACTIVITY = 1;

    /**
     * 油耗零
     */
    private final static double ZERO_OIL = 0D;
    /**
     * 里程零
     */
    private final static double ZERO_MILE = 0D;

    /**
     * 符合条件活动 map的key
     */
    private final static String FIT_CONDITION_ACTIVITY = "fitConditionActivity";

    /**
     * double 保留小数点两位
     */
    private final static String KEEP_THE_DECIMAL_TWO = "0.00";

    /**
     * 日平均油耗 升序降序排序标志位 true为升序从小到大排序，false为降序从大到小排序
     */
    private static boolean DAY_AVG_OILWEAR_UP = Boolean.parseBoolean(PropertiesUtil.getProperties("day.avg.oilwear.up"));

    /**
     * 统计节油大赛，处理流程：
     * <p>
     * 1.获取活动信息，判断活动状态
     * 2.获取有效参赛车辆信息
     * 3.从mongodb查询里程油耗
     * 4.spark计算数据，合并MySQL与mongodb数据，进行排名
     * 5.将spark计算后的数据更新到MySQL中
     */
    public static void main(String[] args) throws ClassNotFoundException {

        /**
         * 本次定时任务运行标志位
         */
        boolean runSpark = false;
        //活动信息 广播变量
        Broadcast<Map<String, OilWearActivityEntity>> activityCast;
        //数据库连接信息 广播变量
        Broadcast<Map<String, String>> dbConfigCast;
        //活动id
        long activityId = 0L;

        //创建SparkConf
        SparkConf sc = new SparkConf()
                //本地运行
                //TODO 上线时候一定要把这个注释掉！！！！！！！！！！！！！！！！！！！！！！！！！！
//                .setMaster("local")
                //应用名称
                .setAppName("OilGame")
                //mongodb input 连接
                .set("spark.mongodb.input.uri", PropertiesUtil.getProperties("spark.mongodb.input.uri"))
                .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner");

        //通过SparkConf 创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sc);

        //初始化MySQL配置
        String mysqlurl = PropertiesUtil.getProperties("mysql.url");
        String username = PropertiesUtil.getProperties("mysql.username");
        String password = PropertiesUtil.getProperties("mysql.password");
        Map<String, String> dbconfig = new HashMap<>();
        dbconfig.put("mysqlurl", mysqlurl);
        dbconfig.put("username", username);
        dbconfig.put("password", password);
        //将数据库连接信息设为广播变量
        dbConfigCast = jsc.broadcast(dbconfig);

        /**
         * 获取活动信息,判断活动状态
         */
        List<OilWearActivityEntity> oList = getActivity(dbconfig);
        logger.error("----------OilGame getActivity size :{}",oList.size());
        //符合条件的活动信息
        Map<String, OilWearActivityEntity> activityMap = new HashMap<>();
        //判断查询出来的活动List应该不等于null 并且size大于零
        if (null != oList && oList.size() > 0) {
            //遍历符合条件的活动
            for (int i = 0; i < oList.size(); i++) {
                OilWearActivityEntity oilWearActivityEntity = oList.get(i);
                /**
                 * 获取活动flag
                 * 活动状态：1:未开始、2:进行中、3:已结束、4:无效
                 */
                activityId = oilWearActivityEntity.getId();
                int activityFlag = oilWearActivityEntity.getActivityFlag();
                Date activityStartDate = oilWearActivityEntity.getActivityStartDate();
                Date activityEndDate = oilWearActivityEntity.getActivityEndDate();
                if (null != activityStartDate && null != activityEndDate){
                    //活动未开始
                    if (ACTIVITY_NOT_BEGIN == activityFlag) {
                        logger.error("------------OilGame activity_not_begin activityId:{}",activityId);
                        String activityStartDateStr = activityStartDate.toString();
                        if (null != activityStartDateStr){
                            int startDateCompare = DateUtil.diffNowDate(activityStartDateStr);
                            //判断活动开始日期 小于当前日期，或者等于当前日期，则把activity_flag改为2，进行中
                            if (SMALL_THAN_NOW == startDateCompare || SAME_AS_NOW == startDateCompare) {
                                logger.error("------------OilGame activity start_date small_than_now activityId:{}",activityId);
                                //更新activity_flag=2把活动改为进行中
                                updateActivityFlag(activityId,ACTIVITYING,ACTIVITY_NOT_BEGIN,dbconfig);
                                logger.error("------------OilGame activity flag update to 2 ,activityId:{}",activityId);
                                //再次判断如果是小于当前时间,则进行spark计算昨天数据
                                if (SMALL_THAN_NOW == startDateCompare){
                                    //进行spark计算昨天数据
                                    runSpark = true;
                                    //将活动信息设置为广播变量
                                    activityMap.put(FIT_CONDITION_ACTIVITY,oilWearActivityEntity);
                                    logger.error("------------OilGame set runSpark == true and set broadcast activityId :{}",activityId);
                                }
                            }
                        }
                    }
                    //活动进行中,肯定是跑spark计算
                    else if (ACTIVITYING == activityFlag){
                        logger.error("------------OilGame activitying activityId:{}",activityId);
                        String activityEndDateStr = activityEndDate.toString();
                        if (null != activityEndDateStr){
                            int endDateCompare = DateUtil.diffNowDate(activityEndDateStr);
                            //如果结束日期比当前日期小，且小于1天或1天以上，则应该把活动activity_flag改为3，活动结束；
                            //因为如果结束日期和当前日期相等就把activity_flag改为3，则会造成 始终最后一天spark不计算
                            //相当于活动结束日期的第二天以上跑定时任务时候，才将activity_flag改为3
                            if(SMALL_THAN_NOW_MORE_ONE == endDateCompare){
                                //更新activity_flag=3 把活动改为 结束
                                updateActivityFlag(activityId,ACTIVITY_DONE,ACTIVITYING,dbconfig);
                                logger.error("------------OilGame activity flag update to 3 ,activityId:{}",activityId);
                            }
                        }
                        //进行spark计算昨天数据
                        runSpark = true;
                        //将活动信息设置为广播变量
                        activityMap.put(FIT_CONDITION_ACTIVITY,oilWearActivityEntity);
                        logger.error("------------OilGame set runSpark == true and set broadcast activityId :{}",activityId);
                    }
                }
            }
        }
        //无活动信息，结束流程
        else {
            return;
        }
        //将符合条件的活动信息设置成(正常来说需要跑spark的活动有且只有一个)
        activityCast = jsc.broadcast(activityMap);
        //根据上面逻辑判断，决定是否进行spark计算
        if (runSpark){
            logger.error("--------------OilGame running spark");
            /**
             * 广播活动车辆信息
             */
            //根据广播变量中的活动id，查询对应车辆信息
            long broadCastActivityId = activityCast.getValue().get(FIT_CONDITION_ACTIVITY).getId();
            Map<String,OilActivityCarMappingEntity> activityCarListMap = getActivityCar(broadCastActivityId, ACTIVITY_CAR_FLAG_NOT_END, dbconfig);
            Broadcast<Map<String,OilActivityCarMappingEntity>> activityCarListCast = jsc.broadcast(activityCarListMap);
            String carIdStr = getCarIdStr(activityCarListMap);
            /**
             * 拼接mongodb查询条件
             */
            int minMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.minMeterInt"));
            int maxMeterInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.maxMeterInt"));
            int minOilInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.minOilInt"));
            int maxOilInt = Integer.parseInt(PropertiesUtil.getProperties("mongodb.maxOilInt"));
            String yesterdayDate = DateUtil.formatDate(new Date(System.currentTimeMillis() - 86400000L));
            logger.error("---------OilGame yesterdayDate :{}",yesterdayDate);
            //拼接mongodb查询条件:bi表中的fuel是当日油耗，meter_gps是当日里程。条件是里程大于10小于2000，平均油耗大于等于5小于等于70。
            String mongoFilter = "{ $match: {date : '" + yesterdayDate + "' ,\"data.meter_gps\":{$gte:" + minMeterInt + ",$lte:" + maxMeterInt + "}," +
                    "\"carId\":{$in:["+carIdStr+"]}} }";
            logger.error("----------OilGame mongoFilter :{}",mongoFilter);
//            String mongoFilter = "{ $match: {\"date\" : '2017-12-01' ,\"data.meter_gps\":{$gte:10,$lte:2000},\"carId\":{$in:[\"80393\",\"67911\"]}}}";
            //拼接mongodb表名 bi_201801(bi_年+昨天月份)
            String mongoTableName = MongoUtil.getLastDayTableName(DateUtil.parseDate(yesterdayDate), MongoUtil.date_pattern2, MongoUtil.MONGO_BI);
            logger.error("----------OilGame mongoTableName :{}",mongoTableName);
//            String mongoTableName = "bi_201712";
            Map<String, String> readOverrides = new HashMap<>();
            readOverrides.put("collection", mongoTableName);
            ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

            Document filter = Document.parse(mongoFilter);
            /**
             * 按照条件查询mongodb返回rdd
             */
            JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(jsc, readConfig).withPipeline(Collections.singletonList(filter));
            /**
             * 处理从mongodb查询出来的数据
             */
            JavaRDD<OilActivityCarMappingEntity> javaRDD = javaMongoRDD.map(new Function<Document, OilActivityCarMappingEntity>() {
                @Override
                public OilActivityCarMappingEntity call(Document document) throws Exception {
                    OilActivityCarMappingEntity retCar = new OilActivityCarMappingEntity();
                    String carId = document.getString("carId");
                    //获取mongodb中车辆信息
                    double mongoCarTotalOilWear = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("fuel")));
                    double mongoCarTotalMileage = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("meter_gps")));
                    //data.fuel / data. meter_gps * 100 >= 5 and data.fuel / data. meter_gps * 100 <= 70
                    //判断从mongodb中获取的里程是否为零，如果为零，则直接忽略该条记录
                    if (ZERO_MILE != mongoCarTotalMileage){
                        //计算当天百公里油耗
                        double fuel_meter = mongoCarTotalOilWear / mongoCarTotalMileage * 100;
                        //如果百公里油耗 大于等于 5 并且 小于等于 70 则进行运算，否者不符合要求，不计算本条记录
                        if (fuel_meter >= minOilInt && fuel_meter <= maxOilInt){
                            if (null != carId){
                                //通过carId获取广播变量中的车辆信息
                                OilActivityCarMappingEntity castCar = activityCarListCast.getValue().get(carId);
                                long id = castCar.getId();
                                double castCarTotalOilWear = castCar.getTotalOilWear();
                                double castCarTotalMileage = castCar.getTotalMileage();

                                //通过广播变量获取活动信息
                                OilWearActivityEntity activity = null;
                                Date activityStart = null;
                                Date activityEnd = null;
                                if (null != activityCast){
                                    activity = activityCast.getValue().get(FIT_CONDITION_ACTIVITY);
                                }
                                if (null != activity){
                                    activityStart = activity.getActivityStartDate();
                                    activityEnd = activity.getActivityEndDate();
                                }
                                //MonthAvgOilWear这个字段只有在活动第一天和活动最后一天+1时候更新
                                //活动第一天计算往期更新到本字段，活动最后一天+1，往期+本期
                                if (null != activityStart && null != activityEnd){
                                    String activityEndPlusOne = DateUtil.formatTime(DateUtil.addDay(DateUtil.parseDate(activityEnd.toString(),DateUtil.date_pattern),1));
                                    //活动开始日期和当前日期相等 计算往期油耗
                                    if (SAME_AS_NOW == DateUtil.diffNowDate(activityStart.toString())){
                                        double lastTermOil = getLastTermOil(carId,ACTIVITY_TABLENAME,ACTIVITY_CAR_TABLENAME,activityStart.toString(),dbConfigCast);
                                        //如果查询出来油耗不为零，则更新该字段
                                        if (ZERO_OIL != lastTermOil){
                                            retCar.setMonthAvgOilWear(lastTermOil);
                                        }
                                    }
                                    //活动结束日期+1 和当前日期相等 计算往期+档期油耗
                                    else if (SAME_AS_NOW == DateUtil.diffNowDate(activityEndPlusOne.toString())){
                                        double allTermOil = getAllTermOil(carId,ACTIVITY_CAR_TABLENAME,dbConfigCast);
                                        if (ZERO_OIL != allTermOil){
                                            retCar.setMonthAvgOilWear(allTermOil);
                                        }
                                    }
                                }
                                /**
                                 * 拼接返回Entity
                                 */
                                retCar.setCarId(carId);
                                //mongo 对应油耗和里程 与MySQL中对应列相加
                                double totalOilWear = mongoCarTotalOilWear + castCarTotalOilWear;
                                double totalMileage = mongoCarTotalMileage + castCarTotalMileage;
                                //MySQL和mongodb加完以后计算日均油耗
                                if (ZERO_MILE != totalMileage){
                                    double dayAvgOilWear = totalOilWear / totalMileage * 100;
                                    //保留两位
                                    DecimalFormat df = new DecimalFormat(KEEP_THE_DECIMAL_TWO);
                                    double dayAvgOilWearTwoDecimal = Double.parseDouble(df.format(dayAvgOilWear));
                                    retCar.setDayAvgOilWear(dayAvgOilWearTwoDecimal);
                                }
                                retCar.setTotalOilWear(totalOilWear);
                                retCar.setTotalMileage(totalMileage);
                                retCar.setId(id);
                                retCar.setModelName(castCar.getModelName());
                            }
                        }
                    }
                    return retCar;
                }
                //过滤百公里油耗 不是 大于等于5 小于等于 70 的数据（因为上面判断条件 如果不在这个范围直接返回new OilActivityCarMappingEntity()这个了，carId应该是null）
            }).filter(new Function<OilActivityCarMappingEntity, Boolean>() {
                @Override
                public Boolean call(OilActivityCarMappingEntity oilActivityCarMappingEntity) throws Exception {
                    if (null != oilActivityCarMappingEntity.getCarId()){
                        return true;
                    }else {
                        return false;
                    }
                }
            });
            /**
             * 将计算完的数据，按照车型名称（model_name）进行转换为Tuple2
             */
            JavaPairRDD<String, OilActivityCarMappingEntity> javaPairRDD = javaRDD.mapToPair(new PairFunction<OilActivityCarMappingEntity, String, OilActivityCarMappingEntity>() {
                @Override
                public Tuple2<String, OilActivityCarMappingEntity> call(OilActivityCarMappingEntity oilActivityCarMappingEntity) throws Exception {
                    return new Tuple2<String, OilActivityCarMappingEntity>(oilActivityCarMappingEntity.getModelName(), oilActivityCarMappingEntity);
                }
            });
            /**
             * 将处理后的数据，进行按照model_name字段分组，并根据日均油耗进行组内排序，排好后将ranking字段更新为顺序号
             */
            JavaRDD<OilActivityCarMappingEntity> rankedRDD = javaPairRDD.groupByKey().mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Iterable<OilActivityCarMappingEntity>>>, OilActivityCarMappingEntity>() {
                @Override
                public List<OilActivityCarMappingEntity> call(Iterator<Tuple2<String, Iterable<OilActivityCarMappingEntity>>> tuple2Iterator) throws Exception {
                    List<OilActivityCarMappingEntity> retList = new ArrayList<>();
                    while (tuple2Iterator.hasNext()){
                        List<OilActivityCarMappingEntity> orderedList = new ArrayList<>();
                        Tuple2<String,Iterable<OilActivityCarMappingEntity>> tuple2 = tuple2Iterator.next();
                        Iterator<OilActivityCarMappingEntity> iterator = tuple2._2.iterator();
                        while (iterator.hasNext()){
                            OilActivityCarMappingEntity oilActivityCarMappingEntity = iterator.next();
                            orderedList.add(oilActivityCarMappingEntity);
                        }
                        if (orderedList.size() > 0){
                            //根据日均油耗进行组内排序
                            Collections.sort(orderedList, new Comparator<OilActivityCarMappingEntity>() {
                                @Override
                                public int compare(OilActivityCarMappingEntity o1, OilActivityCarMappingEntity o2) {
                                    //比较的日均油耗都进行乘以100操作，因为保留小数点两位，因此会比较的更精确
                                    double compareDouble = o1.getDayAvgOilWear() * 100 - o2.getDayAvgOilWear() * 100;
                                   if (DAY_AVG_OILWEAR_UP){
                                       //升序从小到大
                                       if (compareDouble > 0){
                                           return 1;
                                       }
                                       else {
                                           return -1;
                                       }
                                   }else {
                                       //降序从大到小
                                       if (compareDouble > 0){
                                           return -1;
                                       }
                                       else {
                                           return 1;
                                       }
                                   }
                                }
                            });
                            //遍历根据日均油耗排序后的车辆数据，计算排名
                            for (int i = 0; i < orderedList.size(); i++) {
                                OilActivityCarMappingEntity oset = new OilActivityCarMappingEntity();
                                OilActivityCarMappingEntity oget = orderedList.get(i);
                                int rank = i + 1;

                                oset.setId(oget.getId());
                                oset.setTotalMileage(oget.getTotalMileage());
                                oset.setTotalOilWear(oget.getTotalOilWear());
                                oset.setDayAvgOilWear(oget.getDayAvgOilWear());
                                oset.setMonthAvgOilWear(oget.getMonthAvgOilWear());
                                oset.setRanking(rank);
                                retList.add(oset);
                            }
                        }
                    }
                    return retList;
                }
            });
            /**
             * 将spark计算后的数据，批量更新到MySQL数据库中
             */
            batchUpdateActivityCar(rankedRDD.collect(), ACTIVITY_CAR_TABLENAME, dbConfigCast);
        }
        logger.error("-------- OilGame end .............");
    }

    /**
     * 获取正在进行中和未开始的活动信息（activity_flag等于1或者2）
     * @param dbconfig
     * @return
     */
    private static List<OilWearActivityEntity> getActivity(Map<String, String> dbconfig) {
        long s = System.currentTimeMillis();
        List<OilWearActivityEntity> oList = new ArrayList<>();
        try {
            Connection conn = DriverManager.getConnection(dbconfig.get("mysqlurl"), dbconfig.get("username"), dbconfig.get("password"));
            String sql = "SELECT " +
                    "id as id ," +
                    "activity_start_date as activityStartDate," +
                    "activity_end_date as activityEndDate , " +
                    "activity_flag as activityFlag " +
                    "FROM " + ACTIVITY_TABLENAME + " WHERE activity_flag = "+ACTIVITY_NOT_BEGIN+" OR activity_flag = " +ACTIVITYING;
            logger.error("----------------OilGame getActivity sql :{}",sql);
            PreparedStatement prest = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = prest.executeQuery(sql);
            while (rs.next()) {
                long id = rs.getLong("id");
                Date activityStartDate = rs.getDate("activityStartDate");
                Date activityEndDate = rs.getDate("activityEndDate");
                int activityFlag = rs.getInt("activityFlag");

                OilWearActivityEntity oilWearActivityEntity = new OilWearActivityEntity();
                oilWearActivityEntity.setId(id);
                oilWearActivityEntity.setActivityStartDate(activityStartDate);
                oilWearActivityEntity.setActivityEndDate(activityEndDate);
                oilWearActivityEntity.setActivityFlag(activityFlag);

                oList.add(oilWearActivityEntity);
            }
            prest.close();
            conn.close();
            long e = System.currentTimeMillis();
            logger.error("-----------OilGame getActivity size :{} , cost time : {}", oList.size(), (e - s));
        } catch (Exception e) {
            logger.error("OilGame getActivity mysql error");
            e.printStackTrace();
        }
        return oList;
    }

    /**
     * 更新活动标志位
     * @param activityId
     * @param activityFlag
     * @param oldActivityFlag 加上之前活动flag相当于乐观锁，防止并发出问题
     * @param dbconfig
     */
    private static void updateActivityFlag(long activityId ,int activityFlag,int oldActivityFlag,Map<String, String> dbconfig){
        try {
            Connection conn = DriverManager.getConnection(dbconfig.get("mysqlurl"), dbconfig.get("username"), dbconfig.get("password"));
            String sql = "UPDATE "+ACTIVITY_TABLENAME+" SET " +
                    "activity_flag = "+activityFlag+" , " +
                    "update_time = now() " +
                    "WHERE id = "+ activityId +" AND activity_flag = "+oldActivityFlag;
            logger.error("----------------OilGame updateActivityFlag sql :{}",sql);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            logger.error("OilGame updateActivityFlag mysql error");
            e.printStackTrace();
        }
    }

    /**
     * 获取参与活动车辆表，通过活动id
     * 这里默认查询有效的活动车辆（flag=0）
     * @param inputActivityId 入参活动id
     * @param activityCarFlag 活动车辆有效标志位 （是否被终止 0 否 1是）
     * @param dbConfig mysql数据库配置map
     * @return
     */
    private static Map<String,OilActivityCarMappingEntity> getActivityCar(long inputActivityId,int activityCarFlag,Map<String, String> dbConfig){
        long s = System.currentTimeMillis();
        //要返回的参加活动车辆列表map，其中key为carId，value为车信息Entity
        Map<String,OilActivityCarMappingEntity> carListMap = new HashMap<>();
        try {
            Connection conn = DriverManager.getConnection(dbConfig.get("mysqlurl"), dbConfig.get("username"), dbConfig.get("password"));
            String sql = "SELECT " +
                    "id as id," +
                    "activity_id as activityId," +
                    "car_id as carId," +
                    "vin as vin," +
                    "user_id as userId," +
                    "user_phone as userPhone," +
                    "model_code as modelCode," +
                    "model_name as modelName," +
                    "total_mileage as totalMileage," +
                    "total_oil_wear as totalOilWear," +
                    "day_avg_oil_wear as dayAvgOilWear," +
                    "month_avg_oil_wear as monthAvgOilWear," +
                    "ranking as ranking," +
                    "flag as flag," +
                    "create_time as createTime," +
                    "update_time as updateTime " +
                    "FROM "+ACTIVITY_CAR_TABLENAME+" " +
                    "WHERE activity_id = " +inputActivityId+" AND flag = " +activityCarFlag;
            logger.error("----------------OilGame getActivityCar sql :{}",sql);
            PreparedStatement prest = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = prest.executeQuery(sql);
            while (rs.next()) {
                long id = rs.getLong("id");
                long activityId = rs.getLong("activityId");
                String carId = rs.getString("carId");
                String vin = rs.getString("vin");
                long userId = rs.getLong("userId");
                String userPhone = rs.getString("userPhone");
                String modelCode = rs.getString("modelCode");
                String modelName = rs.getString("modelName");
                double totalMileage = rs.getDouble("totalMileage");
                double totalOilWear = rs.getDouble("totalOilWear");
                double dayAvgOilWear = rs.getDouble("dayAvgOilWear");
                double monthAvgOilWear = rs.getDouble("monthAvgOilWear");
                int ranking = rs.getInt("ranking");
                int flag = rs.getInt("flag");
                Date createTime = rs.getDate("createTime");
                Date updateTime = rs.getDate("updateTime");

                OilActivityCarMappingEntity oilWearActivityEntity = new OilActivityCarMappingEntity();
                oilWearActivityEntity.setId(id);
                oilWearActivityEntity.setActivityId(activityId);
                oilWearActivityEntity.setCarId(carId);
                oilWearActivityEntity.setVin(vin);
                oilWearActivityEntity.setUserId(userId);
                oilWearActivityEntity.setUserPhone(userPhone);
                oilWearActivityEntity.setModelCode(modelCode);
                oilWearActivityEntity.setModelName(modelName);
                oilWearActivityEntity.setTotalMileage(totalMileage);
                oilWearActivityEntity.setTotalOilWear(totalOilWear);
                oilWearActivityEntity.setDayAvgOilWear(dayAvgOilWear);
                oilWearActivityEntity.setMonthAvgOilWear(monthAvgOilWear);
                oilWearActivityEntity.setRanking(ranking);
                oilWearActivityEntity.setFlag(flag);
                oilWearActivityEntity.setCreatetTime(createTime);
                oilWearActivityEntity.setUpdateTime(updateTime);

                carListMap.put(carId,oilWearActivityEntity);
            }
            prest.close();
            conn.close();
            long e = System.currentTimeMillis();
            logger.error("-----------OilGame getActivityCar size :{} , cost time : {}", carListMap.size(), (e - s));
        } catch (Exception e) {
            logger.error("getActivityCar mysql error");
            e.printStackTrace();
        }
        return carListMap;
    }

    /**
     * 通过车辆信息列表获取对应列表中carId
     * 返回格式：id以英文逗号分隔 例如：1,2,3
     * @param carListMap
     * @return
     */
    private static String getCarIdStr(Map<String,OilActivityCarMappingEntity> carListMap) {
        StringBuilder result = new StringBuilder();
        String ret;
        if (null != carListMap && carListMap.size() > 0){
            //遍历map，拼接carId
            for (Map.Entry<String,OilActivityCarMappingEntity> entry : carListMap.entrySet()) {
                result.append("\"");
                result.append(String.valueOf(entry.getKey()));
                result.append("\"");
                result.append(",");
            }
        }
        //如果ret不为"",则应该把最后的逗号去掉
        ret = result.toString();
        if (StringUtils.isNotEmpty(ret)) {
            ret = ret.substring(0, ret.length() - 1);
        }
        return ret;
    }

    /**
     * 批量更新活动车辆信息
     * @param activityCarList
     * @param tableName
     * @param dbConfigCast
     */
    private static void batchUpdateActivityCar(List<OilActivityCarMappingEntity> activityCarList , String tableName, Broadcast<Map<String, String>> dbConfigCast){
        if (null== dbConfigCast){
            return;
        }
        Map<String, String> dbConfig = dbConfigCast.getValue();
        if (null != dbConfig && dbConfig.size() > 0){
            try {
                Connection conn;
                conn = DriverManager.getConnection(dbConfig.get("mysqlurl"), dbConfig.get("username"), dbConfig.get("password"));
                conn.setAutoCommit(false);
                Statement statement = conn.createStatement();
                if (null != activityCarList && activityCarList.size() > 0){
                    logger.error("-----------OilGame batchUpdateActivityCar activityCarList size :{}",activityCarList.size());
                    //循环拼接要更新的sql语句
                    for (OilActivityCarMappingEntity oilActivityCarMappingEntity : activityCarList){
                        String sql = "UPDATE "+tableName+" SET " +
                                "total_mileage = "+oilActivityCarMappingEntity.getTotalMileage()+" , " +
                                "total_oil_wear = "+oilActivityCarMappingEntity.getTotalOilWear()+" , " +
                                "day_avg_oil_wear = "+oilActivityCarMappingEntity.getDayAvgOilWear()+" , " +
                                "month_avg_oil_wear = "+oilActivityCarMappingEntity.getMonthAvgOilWear()+" , " +
                                "ranking = "+oilActivityCarMappingEntity.getRanking()+" ," +
                                "update_time = now() " +
                                "WHERE id = "+oilActivityCarMappingEntity.getId();
                        logger.error("----------------OilGame batchUpdateActivityCar sql :{}",sql);
                        statement.addBatch(sql);
                    }
                    statement.executeBatch();
                    conn.commit();
                    conn.close();
                }

            } catch (Exception e) {
                logger.error("OilGame batchUpdateActivityCar mysql error");
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取当期 + 往期油耗信息
     * @param carId
     * @param activityCarTableName
     * @param dbConfigCast
     * @return
     */
    private static double getAllTermOil(String carId, String activityCarTableName,Broadcast<Map<String, String>> dbConfigCast){
        double ret = 0L;
        if (null== dbConfigCast){
            return ret;
        }
        Map<String, String> dbConfig = dbConfigCast.getValue();
        if (null != dbConfig && dbConfig.size() > 0){
            try {
                Connection conn = DriverManager.getConnection(dbConfig.get("mysqlurl"), dbConfig.get("username"), dbConfig.get("password"));
                String sql = "SELECT" +
                        " avg(total_oil_wear) allTermOil " +
                        " FROM  " +activityCarTableName +
                        " WHERE " +
                        "car_id =" + carId +
                        " GROUP BY " +
                        "car_id" +
                        " ORDER BY " +
                        "activity_id DESC";
                logger.error("----------------OilGame getAllTermOil sql :{}",sql);
                PreparedStatement prest = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = prest.executeQuery(sql);
                while (rs.next()) {
                    ret = rs.getDouble("allTermOil");
                }
                prest.close();
                conn.close();
            } catch (Exception e) {
                logger.error("OilGame getAllTermOil mysql error");
                e.printStackTrace();
            }
        }
        return ret;
    }

    /**
     * 获取往期油耗信息
     * @param carId
     * @param activityTableName
     * @param activityCarTableName
     * @param activityStartDate
     * @param dbConfigCast
     * @return
     */
    private static double getLastTermOil(String carId, String activityTableName,String activityCarTableName,String activityStartDate,Broadcast<Map<String, String>> dbConfigCast){
        double ret = ZERO_OIL;
        if (null== dbConfigCast){
            return ret;
        }
        Map<String, String> dbConfig = dbConfigCast.getValue();
        if (null != dbConfig && dbConfig.size() > 0){
            try {
                Connection conn = DriverManager.getConnection(dbConfig.get("mysqlurl"), dbConfig.get("username"), dbConfig.get("password"));
                String sql = "SELECT" +
                        " avg(car.total_oil_wear) lastTermOil" +
                        " FROM " +activityCarTableName+" car" +
                        " INNER JOIN "+activityTableName+" oact ON car.activity_id = oact.id " +
                        " WHERE " +
                        "car.car_id = " +carId+
                        " AND oact.activity_start_date <= '" + activityStartDate+"'";
                logger.error("----------------OilGame getLastTermOil sql :{}",sql);
                PreparedStatement prest = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = prest.executeQuery(sql);
                while (rs.next()) {
                    ret = rs.getDouble("lastTermOil");
                }
                prest.close();
                conn.close();
            } catch (Exception e) {
                logger.error("OilGame getLastTermOil mysql error");
                e.printStackTrace();
            }
        }
        return ret;
    }
}
