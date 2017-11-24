package com.luck.entity;

import java.io.Serializable;

public class CarEntity implements Serializable {
    private String id;
    //通信号
    private String autoTerminal;
    //车型ID
    private String carModel;
    //车牌号
    private String carNumber;

    @Override
    public String toString() {
        return "CarEntity{" +
                "id='" + id + '\'' +
                ", autoTerminal='" + autoTerminal + '\'' +
                ", carModel='" + carModel + '\'' +
                ", carNumber='" + carNumber + '\'' +
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
}