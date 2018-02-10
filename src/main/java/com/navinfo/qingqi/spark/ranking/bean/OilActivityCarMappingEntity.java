package com.navinfo.qingqi.spark.ranking.bean;

import java.io.Serializable;
import java.util.Date;

/**
 * 参与活动车辆表Entity
 * Created by gzh on 2018/1/26 0026.
 */
public class OilActivityCarMappingEntity implements Serializable {

    /**
     * 主键
     */
    private long id;
    /**
     * 活动id
     */
    private long activityId;
    /**
     * 车辆id
     */
    private String carId;
    /**
     * 车辆vin
     */
    private String vin;
    /**
     * 用户自增id
     */
    private long userId;
    /**
     * 用户手机号
     */
    private String userPhone;
    /**
     * 车型码
     */
    private String modelCode;
    /**
     * 车型名称
     */
    private String modelName;
    /**
     * 总里程
     */
    private double totalMileage;
    /**
     * 总油耗
     */
    private double totalOilWear;
    /**
     * 日均油耗
     */
    private double dayAvgOilWear;
    /**
     * 月均油耗
     */
    private double monthAvgOilWear;
    /**
     * 排名
     */
    private int ranking;
    /**
     * 是否被终止 0 否 1是
     */
    private int flag;
    /**
     * 创建时间
     */
    private Date createtTime;
    /**
     * 更新时间
     */
    private Date updateTime;

    @Override
    public String toString() {
        return "oilActivityCarMappingEntity{" +
                "id=" + id +
                ", activityId=" + activityId +
                ", carId='" + carId + '\'' +
                ", vin='" + vin + '\'' +
                ", userId=" + userId +
                ", userPhone='" + userPhone + '\'' +
                ", modelCode='" + modelCode + '\'' +
                ", modelName='" + modelName + '\'' +
                ", totalMileage=" + totalMileage +
                ", totalOilWear=" + totalOilWear +
                ", dayAvgOilWear=" + dayAvgOilWear +
                ", monthAvgOilWear=" + monthAvgOilWear +
                ", ranking=" + ranking +
                ", flag=" + flag +
                ", createtTime=" + createtTime +
                ", updateTime=" + updateTime +
                '}';
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getActivityId() {
        return activityId;
    }

    public void setActivityId(long activityId) {
        this.activityId = activityId;
    }

    public String getCarId() {
        return carId;
    }

    public void setCarId(String carId) {
        this.carId = carId;
    }

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getUserPhone() {
        return userPhone;
    }

    public void setUserPhone(String userPhone) {
        this.userPhone = userPhone;
    }

    public String getModelCode() {
        return modelCode;
    }

    public void setModelCode(String modelCode) {
        this.modelCode = modelCode;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public double getTotalMileage() {
        return totalMileage;
    }

    public void setTotalMileage(double totalMileage) {
        this.totalMileage = totalMileage;
    }

    public double getTotalOilWear() {
        return totalOilWear;
    }

    public void setTotalOilWear(double totalOilWear) {
        this.totalOilWear = totalOilWear;
    }

    public double getDayAvgOilWear() {
        return dayAvgOilWear;
    }

    public void setDayAvgOilWear(double dayAvgOilWear) {
        this.dayAvgOilWear = dayAvgOilWear;
    }

    public double getMonthAvgOilWear() {
        return monthAvgOilWear;
    }

    public void setMonthAvgOilWear(double monthAvgOilWear) {
        this.monthAvgOilWear = monthAvgOilWear;
    }

    public int getRanking() {
        return ranking;
    }

    public void setRanking(int ranking) {
        this.ranking = ranking;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public Date getCreatetTime() {
        return createtTime;
    }

    public void setCreatetTime(Date createtTime) {
        this.createtTime = createtTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
