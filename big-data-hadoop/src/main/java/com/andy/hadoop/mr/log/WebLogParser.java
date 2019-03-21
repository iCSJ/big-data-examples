package com.andy.hadoop.mr.log;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class WebLogParser {

    static SimpleDateFormat sd1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);

    static SimpleDateFormat sd2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 清洗数据
     *
     * @param line
     * @return
     */
    public static WebLogBean parser(String line) {
        WebLogBean webLogBean = new WebLogBean();
        String[] arr = line.split(" ");
        if (arr.length > 11) {
            webLogBean.set(arr[0], arr[1], parseTime(arr[3].substring(1)), arr[6], arr[8], arr[9], arr[10]);

            if (arr.length > 12) {
                webLogBean.setHttp_user_agent(arr[11] + " " + arr[12]);
            } else {
                webLogBean.setHttp_user_agent(arr[11]);
            }
            // 大于400，HTTP错误
            if (Integer.parseInt(webLogBean.getStatus()) >= 400) {
                webLogBean.setValid(false);
            }
        } else {
            webLogBean.setValid(false);
        }
        return webLogBean;
    }

    /**
     * 转换字符串格式的时间
     *
     * @param dt
     * @return
     */
    public static String parseTime(String dt) {
        String timeString = "";
        try {
            Date parse = sd1.parse(dt);
            timeString = sd2.format(parse);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return timeString;
    }

    public static void main(String[] args) {
        WebLogParser wp = new WebLogParser();
        String parseTime = parseTime("18/Sep/2013:06:49:48");
        System.out.println(parseTime);
    }

}
