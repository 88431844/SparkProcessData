package com.luck.util;

import com.luck.entity.CarRankingYesterdayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.List;

/**
 * @Author miracle
 * @Date 2017/11/27 0027 15:19
 */
public class MysqlUtil {

    private static Logger logger = LoggerFactory.getLogger(MysqlUtil.class);

    private static int mysqlBatchSize = Integer.parseInt(PropertiesUtil.getProperties("mysql.batch.size"));

    /**
     * 批量插入MySQL
     */
    public static void batchInsert(List<CarRankingYesterdayEntity> writeToMysql,String tableName){
        long writToMysqlStart = System.currentTimeMillis();
        //平均油耗小数点保留两位
        DecimalFormat df = new DecimalFormat("0.00");
        try {
            Connection conn = DriverManager.getConnection(PropertiesUtil.getProperties("mysql.url"), PropertiesUtil.getProperties("mysql.username"), PropertiesUtil.getProperties("mysql.password"));
            conn.setAutoCommit(false);
            String sql = "INSERT "+tableName+"(" +
                    "car_id," +
                    "car_num," +
                    "statis_date," +
                    "mileage," +
                    "oilwear," +
                    "oilwear_avg," +
                    "create_time," +
                    "statis_timestamp," +
                    "car_model" +
                    ",ranking," +
                    "percentage) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement prest = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

            ListUtil listUtil = new ListUtil();
            //把要插入MySQL的List按照一定数量分割，并且批量插入到MySQL中
            List<List<CarRankingYesterdayEntity>> splitWriteToMysql = listUtil.splitList(writeToMysql, mysqlBatchSize);

            for (int i = 0; i < splitWriteToMysql.size(); i++) {
                for (int x = 0; x < splitWriteToMysql.get(i).size(); x++) {
                    CarRankingYesterdayEntity carRankingYesterdayEntity = splitWriteToMysql.get(i).get(x);
                    prest.setString(1, carRankingYesterdayEntity.getCar_id());
                    prest.setString(2, carRankingYesterdayEntity.getCar_num());
                    prest.setString(3, carRankingYesterdayEntity.getStatis_date());
                    prest.setDouble(4, Double.parseDouble(df.format(carRankingYesterdayEntity.getMileage())));
                    prest.setDouble(5, Double.parseDouble(df.format(carRankingYesterdayEntity.getOilwear())));
                    prest.setDouble(6, Double.parseDouble(df.format(carRankingYesterdayEntity.getOilwear_avg())));
                    prest.setString(7, carRankingYesterdayEntity.getCreate_time());
                    prest.setLong(8, carRankingYesterdayEntity.getStatis_timestamp());
                    prest.setString(9, carRankingYesterdayEntity.getCar_model());
                    prest.setInt(10, carRankingYesterdayEntity.getRanking());
                    prest.setDouble(11, carRankingYesterdayEntity.getPercentage());
                    prest.addBatch();
                }
                prest.executeBatch();
                conn.commit();
            }
            conn.close();
        } catch (Exception e) {
            logger.error("mysql insert error");
            e.printStackTrace();
        }
        long writToMysqlEnd = System.currentTimeMillis();
        logger.info("-----------writeToMysql size :{} , cost time : {}",writeToMysql.size(),(writToMysqlEnd - writToMysqlStart));
    }

    /**
     * 删除对应的油量排行数据
     * mysqlTableName 是要删除的表名（昨日：car_ranking_yesterday，上周：car_ranking_week，上个月：car_ranking_month）
     * statisTimestamp 是统计日期当天零点的时间戳十三位（昨日，上周：周一，上个月：月初一号）
     * @param mysqlTableName
     * @param statisTimestamp
     */
    public static void delRakingData(String mysqlTableName,Long statisTimestamp){
        try {
            Connection conn = DriverManager.getConnection(PropertiesUtil.getProperties("mysql.url"), PropertiesUtil.getProperties("mysql.username"), PropertiesUtil.getProperties("mysql.password"));
            String sql = "delete from "+mysqlTableName+" where statis_timestamp = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt = conn.prepareStatement(sql);
            //为占位符赋值
            pstmt.setLong(1, statisTimestamp);
            pstmt.executeUpdate();
        }catch (Exception e){
            logger.error("MysqlUtil delRakingData error");
            e.printStackTrace();
        }

    }
}
