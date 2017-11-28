package com.luck.util;

import com.luck.entity.CarRankingYesterdayEntity;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.Document;
import scala.Tuple2;

import java.util.*;

/**
 * @Author miracle
 * @Date 2017/11/28 0028 14:16
 */
public class SparkUtil {

    /**
     *  计算油耗排行
     * 入参是JavaRDD 返回处理后的 List<CarRankingYesterdayEntity>
     * @param rdd
     * @param statisDate
     * @param type 1是昨日油耗排行；2是上个月油耗排行；3是上周油耗排行
     * @return
     */
    private static List<CarRankingYesterdayEntity> getListFromRDD(JavaRDD rdd,String statisDate,String type){
        List<CarRankingYesterdayEntity> retList = new ArrayList<>();
        //处理从mongodb查询出来的数据
        JavaRDD<CarRankingYesterdayEntity> javaRDD = rdd.map(new Function<Document, CarRankingYesterdayEntity>() {
            @Override
            public CarRankingYesterdayEntity call(Document document) throws Exception {
                CarRankingYesterdayEntity carRankingYesterdayEntity = new CarRankingYesterdayEntity();
                String terminalId = String.valueOf(document.getLong("terminalId"));
                if (null != terminalId){
                    //通过terminalId获取车辆信息
                    HashMap<String, String> carMap = CarCache.getCar(terminalId);
                    if (null != carMap){
                        String carModel = carMap.get("carModel");
                        String carNumber = carMap.get("carNumber");
                        String id = carMap.get("id");

                        double fuel = 0D;
                        double meterGps = 0D;
                        //判断是什么类型油耗排行计算：1是昨日油耗排行；2是上个月油耗排行；3是上周油耗排行
                        if (type.equals("1")){
                            fuel = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("fuel")));
                            meterGps = Double.parseDouble(String.valueOf(JsonUtil.fromJson(JsonUtil.toJson(document.get("data")), Map.class).get("meter_gps")));
                        }
                        else if(type.equals("2")){
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
     * @param rdd
     * @param statisDate
     * @return
     */
    public static List<CarRankingYesterdayEntity> getListFromRDDYesterday(JavaRDD rdd,String statisDate){
        return getListFromRDD(rdd,statisDate,"1");
    }

    /**
     * 计算上个月里程油耗排行
     * @param rdd
     * @param statisDate
     * @return
     */
    public static List<CarRankingYesterdayEntity> getListFromRDDLastMonth(JavaRDD rdd,String statisDate){
        return getListFromRDD(rdd,statisDate,"2");
    }

}
