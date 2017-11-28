package com.luck.util;

import com.luck.entity.CarEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.sql.*;
import java.util.HashMap;


/**
 * @Author miracle
 * @Date 2017/11/21 0021 16:50
 */
public class CarCache implements Serializable {

    private static HashMap<String, HashMap<String,String>> cache = new HashMap<>();

    private static Logger logger = LoggerFactory.getLogger(CarCache.class);

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
    public static HashMap<String,String> getCar(String autoTerminal){
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
     * 初始化所有车辆信息
     * @return
     */
    public static HashMap<String, HashMap<String, String>> initCache() {
        long loadAllCarStart = System.currentTimeMillis();
        CarCache carCache = new CarCache();
        String driver = "com.mysql.jdbc.Driver";
        // URL指向要访问的数据库名game
        String url = PropertiesUtil.getProperties("mysql.url");
        // MySQL配置时的用户名
        String user = PropertiesUtil.getProperties("mysql.username");
        // MySQL配置时的密码
        String password = PropertiesUtil.getProperties("mysql.password");
        try {
            Class.forName(driver);
            try (
                    Connection conn = DriverManager.getConnection(url, user, password);
                    Statement statement = conn.createStatement();
                    //查询所有车信息，排除carModel为null的数据
                    ResultSet rs = statement.executeQuery("SELECT id, auto_terminal as autoTerminal,car_model as carModel,car_number as carNumber FROM car WHERE car_model is not null")
            ) {
                while (rs.next()) {
                    String id = rs.getString("id");
                    String autoTerminal = rs.getString("autoTerminal");
                    String carModel = rs.getString("carModel");
                    String carNumber = rs.getString("carNumber");
                    CarEntity carEntity = new CarEntity();
                    carEntity.setAutoTerminal(autoTerminal);
                    carEntity.setCarModel(carModel);
                    carEntity.setCarNumber(carNumber);
                    carEntity.setId(id);
                    carCache.addCar(carEntity);
                }
            }
        } catch (Exception e) {
            logger.error("Exception", e);
            e.printStackTrace();
        }
        long loadAllCarEnd = System.currentTimeMillis();
        logger.info("-----------get all car from mysql size : {}  ,cost time : {}",carCache.getCache().size(),(loadAllCarEnd - loadAllCarStart));
        return cache;
    }

    public static HashMap<String, HashMap<String, String>> getCache() {
        return cache;
    }
}
