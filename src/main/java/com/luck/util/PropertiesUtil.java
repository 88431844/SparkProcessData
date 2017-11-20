package com.luck.util;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Created by yangmg on 2016/3/7.
 */
public class PropertiesUtil {
    private static final Properties properties = new Properties();
    private static Logger logger = Logger.getLogger(PropertiesUtil.class);
    private static InputStream inputStream = null;
    private static final String FILE_NAME = "config.properties";
    private static Map<String,String> allParam = new HashMap<String, String>();

    public static Properties loadProperty (){
        inputStream = PropertiesUtil.class.getClassLoader().getResourceAsStream(FILE_NAME);
        try {
            properties.load(inputStream);

        } catch (IOException e) {
            logger.error("init"+FILE_NAME+"error",e);
            logger.error(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.error("init error"+FILE_NAME+"close stream fail",e);
                logger.error(e);
            }
        }
        return properties;
    }

    //根据文件名称-key，返回相应key的值
    public static String getPropertiesByKey(String fileName,String key){
        try {
            if(allParam.containsKey(key)){
                return allParam.get(key);
            }else{
                logger.info("开始初始化配置文件【"+fileName+"】");
                allParam.clear();
                InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName);
                Properties p = new Properties();
                p.load(in);
                Set<Entry<Object, Object>> allKey = p.entrySet();
                for (Entry<Object, Object> entry : allKey) {
                    allParam.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
                in.close();
                logger.info("成功初始化配置文件【"+fileName+"】");
                return allParam.get(key);
            }
        } catch (Exception e) {
            logger.error("初始化配置文件【"+fileName+"】出错");
            e.printStackTrace();
        }
        return null;
    }


    public static Map<String,String> getProperties(){
        try {
            if(allParam.size()==0){

                InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream(FILE_NAME);
                Properties p = new Properties();
                p.load(in);
                Set<Entry<Object, Object>> allKey = p.entrySet();
                for (Entry<Object, Object> entry : allKey) {
                    allParam.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
                in.close();
                return allParam;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return allParam;
    }

    public static String getProperties(String key){
        return getPropertiesByKey(FILE_NAME, key);
    }
}
