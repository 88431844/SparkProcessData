package com.luck.util;

import com.luck.entity.CarEntity;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @Author miracle
 * @Date 2017/11/21 0021 16:50
 */
public class CarCache implements Serializable {

    private static HashMap<String, HashMap<String,String>> cache = new HashMap<>();

    public CarCache(){

    }
    /**
     * 添加瓦片与区域关系
     * @param carEntity
     */
    public void addCar(CarEntity carEntity) {
        HashMap<String,String> carMap = new HashMap<>();
        carMap.put("id",carEntity.getId());
        carMap.put("autoTerminal",carEntity.getAutoTerminal());
        carMap.put("carModel",carEntity.getCarModel());
        carMap.put("carNumber",carEntity.getCarNumber());
        cache.put(carEntity.getAutoTerminal(),carMap);
    }

    /**
     * 通过autoTerminal获取car信息
     * @param autoTerminal
     * @return
     */
    public HashMap<String,String> getCar(String autoTerminal){
        return cache.get(autoTerminal);
    }

    /**
     * 通过autoTerminal移除car信息
     * @param autoTerminal
     */
    public void removeCar(String autoTerminal){
        cache.remove(autoTerminal);
    }

    /**
     * 获取所有车辆信息
     * @return
     */
    public HashMap<String, HashMap<String, String>> getCache() {
        return cache;
    }

}
