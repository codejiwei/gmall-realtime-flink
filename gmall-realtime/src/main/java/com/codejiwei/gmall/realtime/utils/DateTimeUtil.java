package com.codejiwei.gmall.realtime.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @ClassName DateTimeUtil
 * @Author codejiwei
 * @Date 2021/2/21 19:51
 * @Version 1.0
 * @Description :
 **/
public class DateTimeUtil {
    public static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /*
     * @Description //TODO 将时间字符串转换成Long类型毫秒数
     * @Param [dateStr]
     * @return java.lang.Long
     **/
    public static Long getTs(String dateStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, dtf);
        long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }

    /*
     * @Description //TODO 将Date格式时间转换成时间字符串String
     * @Param [date]
     * @return java.lang.String
     **/
    public static String toYMDhms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }
}
