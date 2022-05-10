package com.fyc.tools;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public  class datatimeTools {
    public static String UTC2BJ(String value) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(value));
        calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + 8);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = calendar.getTime();
        String date2 = simpleDateFormat.format(calendar.getTime());
        return date2;
    }
}
