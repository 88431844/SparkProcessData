package com.navinfo.qingqi.spark.ranking.bean;

import java.io.Serializable;
import java.util.Date;

/**
 * 活动表
 * Created by gzh on 2018/1/25 0025.
 */
public class OilWearActivityEntity implements Serializable{
    /**
     * 活动id
     */
    private long id;
    /**
     * 活动开始时间
     */
    private Date activityStartDate;
    /**
     * 活动结束时间
     */
    private Date activityEndDate;

    /**
     * 活动状态：
     1:未开始
     2:进行中
     3:已结束
     4:无效
     */
    private int activityFlag;

    @Override
    public String toString() {
        return "OilWearActivityEntity{" +
                "id=" + id +
                ", activityStartDate=" + activityStartDate +
                ", activityEndDate=" + activityEndDate +
                ", activityFlag=" + activityFlag +
                '}';
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Date getActivityStartDate() {
        return activityStartDate;
    }

    public void setActivityStartDate(Date activityStartDate) {
        this.activityStartDate = activityStartDate;
    }

    public Date getActivityEndDate() {
        return activityEndDate;
    }

    public void setActivityEndDate(Date activityEndDate) {
        this.activityEndDate = activityEndDate;
    }

    public int getActivityFlag() {
        return activityFlag;
    }

    public void setActivityFlag(int activityFlag) {
        this.activityFlag = activityFlag;
    }
}
