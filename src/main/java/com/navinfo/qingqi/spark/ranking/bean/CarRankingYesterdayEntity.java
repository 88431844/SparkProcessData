package com.navinfo.qingqi.spark.ranking.bean;

import java.io.Serializable;

/**
 * @author miracle
 * @Date 2017/11/21 0021 16:21
 */
public class CarRankingYesterdayEntity implements Serializable {

    //数据id
    private long id;
    //车辆唯一标识ID
    private String car_id;
    //车牌号
    private String car_num;
    //统计日期(昨日；周一，月第一天)
    private String statis_date;
    //当天的行程里程
    private double mileage;
    //当天的总油耗
    private double oilwear;
    //当天的百公里油耗
    private double oilwear_avg;
    //创建时间
    private String create_time;
    //统计日期零点时间戳(昨日；周一，月第一天)
    private long statis_timestamp;
    //车型ID
    private String car_model;
    //同车型排名
    private int ranking;
    //超过的百分比
    private double percentage;

    //车型名称
    private String model_name;

    //统计合格天数
    private int vilidday;

    @Override
    public String toString() {
        return "CarRankingYesterdayEntity{" +
                "id=" + id +
                ", car_id='" + car_id + '\'' +
                ", car_num='" + car_num + '\'' +
                ", statis_date='" + statis_date + '\'' +
                ", mileage=" + mileage +
                ", oilwear=" + oilwear +
                ", oilwear_avg=" + oilwear_avg +
                ", create_time='" + create_time + '\'' +
                ", statis_timestamp=" + statis_timestamp +
                ", car_model='" + car_model + '\'' +
                ", ranking=" + ranking +
                ", percentage=" + percentage +
                ", model_name='" + model_name + '\'' +
                ", vilidday=" + vilidday +
                '}';
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getCar_id() {
        return car_id;
    }

    public void setCar_id(String car_id) {
        this.car_id = car_id;
    }

    public String getCar_num() {
        return car_num;
    }

    public void setCar_num(String car_num) {
        this.car_num = car_num;
    }

    public String getStatis_date() {
        return statis_date;
    }

    public void setStatis_date(String statis_date) {
        this.statis_date = statis_date;
    }

    public double getMileage() {
        return mileage;
    }

    public void setMileage(double mileage) {
        this.mileage = mileage;
    }

    public double getOilwear() {
        return oilwear;
    }

    public void setOilwear(double oilwear) {
        this.oilwear = oilwear;
    }

    public double getOilwear_avg() {
        return oilwear_avg;
    }

    public void setOilwear_avg(double oilwear_avg) {
        this.oilwear_avg = oilwear_avg;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public long getStatis_timestamp() {
        return statis_timestamp;
    }

    public void setStatis_timestamp(long statis_timestamp) {
        this.statis_timestamp = statis_timestamp;
    }

    public String getCar_model() {
        return car_model;
    }

    public void setCar_model(String car_model) {
        this.car_model = car_model;
    }

    public int getRanking() {
        return ranking;
    }

    public void setRanking(int ranking) {
        this.ranking = ranking;
    }

    public double getPercentage() {
        return percentage;
    }

    public void setPercentage(double percentage) {
        this.percentage = percentage;
    }

    public String getModel_name() {
        return model_name;
    }

    public void setModel_name(String model_name) {
        this.model_name = model_name;
    }

    public int getVilidday() {
        return vilidday;
    }

    public void setVilidday(int vilidday) {
        this.vilidday = vilidday;
    }
}
