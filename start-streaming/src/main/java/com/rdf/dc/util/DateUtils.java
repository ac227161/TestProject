package com.rdf.dc.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtils {
    private static ThreadLocal<DateFormat> DATE_FORMATTER = new ThreadLocal<>();
    public static final DateTimeFormatter DAY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHH");

    public static DateFormat getDateFormat() {
        DateFormat df = DATE_FORMATTER.get();
        if (df == null) {
            df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            DATE_FORMATTER.set(df);
        }
        return df;
    }

    public static String dateToString(Date datetime) {
        return getDateFormat().format(datetime);
    }

    public static String unixToDateTimeStr(long unixTime, DateTimeFormatter formatter) {
        Instant instant = Instant.ofEpochSecond(unixTime);
        ZoneId zoneId = ZoneId.systemDefault();
        String dateStr = LocalDateTime.ofInstant(instant, zoneId).format(formatter);
        return dateStr;
    }
}
