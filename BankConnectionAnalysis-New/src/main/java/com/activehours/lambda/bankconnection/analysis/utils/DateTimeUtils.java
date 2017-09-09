package com.activehours.lambda.bankconnection.analysis.utils;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateTimeUtils {
    public static Date getCurrentDateInEST() {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
        return new Date(calendar.getTimeInMillis());
    }
}
