package com.navinfo.qingqi.spark.ranking.bean;

import java.io.Serializable;

/**
 * @author admin
 */
public class CarEntity implements Serializable {
    private String id;

    /**
     * 通信号
     */
    private String autoTerminal;

    /**
     * 车型ID
     */
    private String carModel;

    /**
     * 车牌号
     */
    private String carNumber;

    /**
     *
     * 车型名称
     */
    private String modelName;

    @Override
    public String toString() {
        return "CarEntity{" +
                "id='" + id + '\'' +
                ", autoTerminal='" + autoTerminal + '\'' +
                ", carModel='" + carModel + '\'' +
                ", carNumber='" + carNumber + '\'' +
                ", modelName='" + modelName + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAutoTerminal() {
        return autoTerminal;
    }

    public void setAutoTerminal(String autoTerminal) {
        this.autoTerminal = autoTerminal;
    }

    public String getCarModel() {
        return carModel;
    }

    public void setCarModel(String carModel) {
        this.carModel = carModel;
    }

    public String getCarNumber() {
        return carNumber;
    }

    public void setCarNumber(String carNumber) {
        this.carNumber = carNumber;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
}