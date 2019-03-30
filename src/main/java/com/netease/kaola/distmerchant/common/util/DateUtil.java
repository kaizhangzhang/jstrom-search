package com.netease.kaola.distmerchant.common.util;

import org.apache.commons.lang.time.FastDateFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author kai.zhang
 * @description 时间操作工具
 * @since 2019/3/4
 */
public class DateUtil {
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DAY_TIME_FORMAT = "yyyy-MM-dd";

    public static Date dateAddDays(Date date, int addDays) {
        Date newDate = null;
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, addDays);
        newDate = cal.getTime();
        return newDate;
    }

    /**
     * 线程安全的时间格式化
     * @param target
     * @param format
     * @return
     */
    public static String safeDateFormat(Date target, String format) {
        if (target == null) {
            return null;
        }
        return FastDateFormat.getInstance(format).format(target);
    }

    public static Date parseDate(String source, String format) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = sdf.parse(source);
        return date;
    }
}
