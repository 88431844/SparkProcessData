package com.luck.entity;

import java.io.Serializable;

/**
 * @author Administrator
 * @date 2017-04-24
 * @modify
 */

public class CountEntity implements Serializable {

    private long terminalId;
    /**
     * 数据日期
     */
    private String date;

    private String carId;

    private long timestamp;

    /**
     * 速度值的累加值
     */
    private long speedSum;
    /**
     * 速度个数
     */
    private int speedNum;

    private BestEntity data;

    public long getSpeedSum() {
        return speedSum;
    }

    public void setSpeedSum(long speedSum) {
        this.speedSum = speedSum;
    }

    public int getSpeedNum() {
        return speedNum;
    }

    public void setSpeedNum(int speedNum) {
        this.speedNum = speedNum;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getCarId() {
        return carId;
    }

    public void setCarId(String carId) {
        this.carId = carId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public long getTerminalId() {
        return terminalId;
    }

    public void setTerminalId(long terminalId) {
        this.terminalId = terminalId;
    }

    public BestEntity getData() {
        return data;
    }

    public void setData(BestEntity data) {
        this.data = data;
    }
}
