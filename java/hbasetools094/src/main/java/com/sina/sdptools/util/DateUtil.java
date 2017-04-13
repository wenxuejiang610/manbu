package com.sina.sdptools.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
  public static String millisecond2date(long time){
    Date d = new Date(time);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return sdf.format(d);
  }
}
