package com.luck.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @Author miracle
 * @Date 2017/11/27 0027 13:38
 */
public class MongoUtil {

    public static final String date_pattern1 = "yyyy-MM-dd";
    public static final String date_pattern2 = "yyyyMM";
    public static final String MONGO_BI = "bi";
    /**
     * 获取传入日期的 昨天mongodb表名
     *
     * @param date
     * @param dateForm
     * @return
     */
    public static String getLastDayTableName(Date date, String dateForm, String mongoName) {
        //如果时间大于今天设置为当前
        if (new Date().getTime() <= date.getTime()) {
            date = new Date();
        }
        //昨日直接减去一天后取月份
        return mongoName + "_" + new SimpleDateFormat(dateForm).format(date.getTime() - 86400000L);
    }

    /**
     * 获取传入日期的 上周mongodb表名
     *
     * @param date
     * @param dateForm
     * @return
     */
    public static List<String> getLastWeekTableName(Date date, String dateForm, String mongoName) {
        List<String> returnList = new ArrayList<>();
        //上周有可能跨月，要进行处理
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.setTime(date);
        cal.add(Calendar.WEEK_OF_YEAR, -1);
        String biLastName = "";
        for (int i = 0; i < 7; i++) {
            String nameTemp = new SimpleDateFormat(dateForm).format(cal.getTime());
            if (!biLastName.equals(nameTemp)) {
                returnList.add(mongoName + "_" + nameTemp);
            }
            biLastName = nameTemp;
            cal.add(Calendar.DAY_OF_YEAR, 1);
        }
        return returnList;
    }

    /**
     * 获取传入日期的 上个月mongodb表名
     *
     * @param date
     * @param dateForm
     * @return
     */
    public static String getLastMonthTableName(Date date, String dateForm, String mongoName) {
        //上个月用Calendar取上个月即可
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, -1);
        return mongoName + "_" + new SimpleDateFormat(dateForm).format(cal.getTime());
    }

    /**
     * 获取传入日期的 昨天mongodb查询条件
     *
     * @param date
     * @param dateForm
     * @return
     */
    public static String getLastDayCondition(Date date, String dateForm) {
        return new SimpleDateFormat(dateForm).format(date.getTime() - 86400000L);
    }

    /**
     * 获取传入日期的 上周mongodb查询条件
     *
     * @param date
     * @param dateForm
     * @return
     */
    public static List<String> getLastWeekCondition(Date date, String dateForm) {
        List<String> rlist = new ArrayList<>();
        rlist.add(getLastWeek(date, Calendar.MONDAY, dateForm));
        rlist.add(getLastWeek(date, Calendar.SUNDAY, dateForm));
        return rlist;
    }

    /**
     * 获取传入日期的 上个月mongodb查询条件
     *
     * @param date
     * @param dateForm
     * @return
     */
    public static List<String> getLastMonthCondition(Date date, String dateForm) {
        List<String> rlist = new ArrayList<>();
        String begin = null;
        String end = null;
        //如果时间大于今天设置为当前
        if (new Date().getTime() <= date.getTime()) {
            date = new Date();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, -1);

        cal.set(Calendar.DAY_OF_MONTH, 1);
        Date firstDayOfMonth = cal.getTime();
        begin = new SimpleDateFormat(dateForm).format(firstDayOfMonth);

        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.add(Calendar.MONTH, 1);
        cal.add(Calendar.DAY_OF_MONTH, -1);
        Date lastDayOfMonth = cal.getTime();
        end = new SimpleDateFormat(dateForm).format(lastDayOfMonth);

        if (null != begin && null != end) {
            rlist.add(begin);
            rlist.add(end);
        }
        return rlist;
    }

    /**
     * 获取上周的日期
     *
     * @param date
     * @param week
     * @return
     */
    private static String getLastWeek(Date date, int week, String dateFrom) {
        //如果时间大于今天设置为当前
        if (new Date().getTime() <= date.getTime()) {
            date = new Date();
        }
        date = new Date(date.getTime() - 86400000L * 7);
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.setTime(date);
        cal.set(Calendar.DAY_OF_WEEK, week);
        String weekday = new SimpleDateFormat(dateFrom).format(cal.getTime());
        //System.out.println(weekday);
        return weekday;
    }

    public static void main(String[] args) {
//        System.out.println(getLastDayTableName(DateUtil.parseDate("2017-11-01"),date_pattern2,"bi"));、
//        System.out.println(getLastMonthTableName(DateUtil.parseDate("2017-01-01"),date_pattern2,"bi"));
//        System.out.println(getLastWeekTableName(DateUtil.parseDate("2017-11-06"),date_pattern2,"bi"));
//        System.out.println(getLastDayCondition(DateUtil.parseDate("2017-11-02"),date_pattern1));
//        System.out.println(getLastWeekCondition(DateUtil.parseDate("2017-11-22"),date_pattern1));
//        System.out.println(MongoUtil.getLastDayTableName(new Date(), MongoUtil.date_pattern2, "bi"));
//        System.out.println(getLastMonthCondition(new Date(),date_pattern1));
//        System.out.println(DateUtil.lastDayOfCurrentMonth("2017-10-30"));
//        System.out.println(DateUtil.firstDayOfCurrentMonth("2017-11-06"));
//        System.out.println(getLastWeekTableName(new Date(),date_pattern2,"bi"));
//        System.out.println(new SimpleDateFormat(date_pattern1).format(DateUtil.parseDate("2017-12-11").getTime() - 86400000L*70));
//        System.out.println(new SimpleDateFormat(date_pattern1).format(DateUtil.parseDate("2017-11-07").getTime() - 86400000L*30));
//        System.out.println(DateUtil.returnSomeDay("2017-11-06",30,"yyyy-MM-dd HH:mm:ss"));
    }
}
