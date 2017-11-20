package com.luck.entity;

import java.io.Serializable;

/**
 * @author Administrator
 * @date 2017-04-24
 * @modify
 */

public class BestEntity implements Serializable {

    /**
     * 车牌号
     */
    private String vehicle_no;
    /**
     * 数据日期
     */
    private String data_date;

    /**
     * 当日仪表里程
     */
    private int meter_milage;
    /**
     * 当日GPS里程
     */
    private double meter_gps;
    /**
     * 当日油耗量
     */
    private double fuel;
    /**
     * 当日工作时长 秒
     */
    private double work_hours;
    /**
     * 当日怠速时长 秒
     */
    private double idle_hours;
    /**
     * 急刹车次数
     */
    private int stbo_times;
    /**
     * 急加速次数
     */
    private int acce_times;
    /**
     * 超速次数
     */
    private int overspeed_times;
    /**
     * 空挡滑行次数
     */
    private int hsns_times;
    /**
     * 疲劳驾驶次数
     */
    private int td_times;
    /**
     * 月均百公里油耗
     */
    private double Fuel_rate_month;
    /**
     * 本月累计仪表里程
     */
    private int milage_meter_month_total;
    /**
     * 本月累计GPS里程
     */
    private double milage_gps_month_total;
    /**
     * 本月累计油耗
     */
    private double fuel_month_total;
    /**
     * 本月累计工作
     */
    private double mork_hours_month_total;
    /**
     * 本月累计怠速
     */
    private double Idle_hours_month_total;
    /**
     * 本月急刹车次数
     */
    private int acce_times_month_total;
    /**
     * 本月空挡滑行次数
     */
    private int hsns_times_month_total;
    /**
     * 平均车速(不含怠速)
     */
    private double avg_speed_noidle;
    /**
     * 平均车速(含怠速)
     */
    private double avg_speed;
    /**
     * 怠速占比
     */
    private double idle_per;
    /**
     * 日均空挡滑行
     */
    private double avg_hsns_day;
    /**
     * 超转速
     */
    private int hes_times;
    /**
     * 经济区占比
     */
    private double ofe_per;
    /**
     * 刹车次数
     */
    private int brake_times;
    /**
     * 超速区占比
     */
    private double overspeed_per;

    public String getVehicle_no() {
        return vehicle_no;
    }

    public void setVehicle_no(String vehicle_no) {
        this.vehicle_no = vehicle_no;
    }

    public String getData_date() {
        return data_date;
    }

    public void setData_date(String data_date) {
        this.data_date = data_date;
    }

    public int getMeter_milage() {
        return meter_milage;
    }

    public void setMeter_milage(int meter_milage) {
        this.meter_milage = meter_milage;
    }

    public double getMeter_gps() {
        return meter_gps;
    }

    public void setMeter_gps(double meter_gps) {
        this.meter_gps = meter_gps;
    }

    public int getStbo_times() {
        return stbo_times;
    }

    public void setStbo_times(int stbo_times) {
        this.stbo_times = stbo_times;
    }

    public int getAcce_times() {
        return acce_times;
    }

    public void setAcce_times(int acce_times) {
        this.acce_times = acce_times;
    }

    public int getOverspeed_times() {
        return overspeed_times;
    }

    public void setOverspeed_times(int overspeed_times) {
        this.overspeed_times = overspeed_times;
    }

    public int getHsns_times() {
        return hsns_times;
    }

    public void setHsns_times(int hsns_times) {
        this.hsns_times = hsns_times;
    }

    public int getTd_times() {
        return td_times;
    }

    public void setTd_times(int td_times) {
        this.td_times = td_times;
    }



    public int getMilage_meter_month_total() {
        return milage_meter_month_total;
    }

    public void setMilage_meter_month_total(int milage_meter_month_total) {
        this.milage_meter_month_total = milage_meter_month_total;
    }

    public double getMilage_gps_month_total() {
        return milage_gps_month_total;
    }

    public void setMilage_gps_month_total(double milage_gps_month_total) {
        this.milage_gps_month_total = milage_gps_month_total;
    }

    public double getFuel() {
        return fuel;
    }

    public void setFuel(double fuel) {
        this.fuel = fuel;
    }

    public double getWork_hours() {
        return work_hours;
    }

    public void setWork_hours(double work_hours) {
        this.work_hours = work_hours;
    }

    public double getIdle_hours() {
        return idle_hours;
    }

    public void setIdle_hours(double idle_hours) {
        this.idle_hours = idle_hours;
    }

    public double getFuel_rate_month() {
        return Fuel_rate_month;
    }

    public void setFuel_rate_month(double fuel_rate_month) {
        Fuel_rate_month = fuel_rate_month;
    }

    public double getFuel_month_total() {
        return fuel_month_total;
    }

    public void setFuel_month_total(double fuel_month_total) {
        this.fuel_month_total = fuel_month_total;
    }

    public double getMork_hours_month_total() {
        return mork_hours_month_total;
    }

    public void setMork_hours_month_total(double mork_hours_month_total) {
        this.mork_hours_month_total = mork_hours_month_total;
    }

    public double getIdle_hours_month_total() {
        return Idle_hours_month_total;
    }

    public void setIdle_hours_month_total(double idle_hours_month_total) {
        Idle_hours_month_total = idle_hours_month_total;
    }

    public int getAcce_times_month_total() {
        return acce_times_month_total;
    }

    public void setAcce_times_month_total(int acce_times_month_total) {
        this.acce_times_month_total = acce_times_month_total;
    }

    public int getHsns_times_month_total() {
        return hsns_times_month_total;
    }

    public void setHsns_times_month_total(int hsns_times_month_total) {
        this.hsns_times_month_total = hsns_times_month_total;
    }

    public double getAvg_speed_noidle() {
        return avg_speed_noidle;
    }

    public void setAvg_speed_noidle(double avg_speed_noidle) {
        this.avg_speed_noidle = avg_speed_noidle;
    }

    public double getAvg_speed() {
        return avg_speed;
    }

    public void setAvg_speed(double avg_speed) {
        this.avg_speed = avg_speed;
    }

    public double getIdle_per() {
        return idle_per;
    }

    public void setIdle_per(double idle_per) {
        this.idle_per = idle_per;
    }

    public double getAvg_hsns_day() {
        return avg_hsns_day;
    }

    public void setAvg_hsns_day(double avg_hsns_day) {
        this.avg_hsns_day = avg_hsns_day;
    }

    public int getHes_times() {
        return hes_times;
    }

    public void setHes_times(int hes_times) {
        this.hes_times = hes_times;
    }

    public double getOfe_per() {
        return ofe_per;
    }

    public void setOfe_per(double ofe_per) {
        this.ofe_per = ofe_per;
    }

    public int getBrake_times() {
        return brake_times;
    }

    public void setBrake_times(int brake_times) {
        this.brake_times = brake_times;
    }

    public double getOverspeed_per() {
        return overspeed_per;
    }

    public void setOverspeed_per(double overspeed_per) {
        this.overspeed_per = overspeed_per;
    }
}
