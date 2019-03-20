package com.andy.hbase.ip;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-18
 **/
public class IpAddress {

    private static final Integer TIME_OUT = 1000;

    public static String INTRANET_IP = getIntranetIp(); // 内网IP

    public static String INTERNET_IP = getV4IP(); // 外网IP

    /**
     * 获取外网的ip地址
     *
     * @throws Exception
     */
    public static void getIpLocation() throws Exception {
        // GeoIP2-City 数据库文件
        String path = ClassLoader.getSystemResource("GeoLite2-City.mmdb").getPath();
        File file = new File(path);

        // 创建 DatabaseReader对象
        DatabaseReader reader = new DatabaseReader.Builder(file).build();

        InetAddress ipAddress = InetAddress.getByName("120.197.48.146");

        CityResponse response = reader.city(ipAddress);

        // 国家
        Country country = response.getCountry();
        System.out.println(country.getIsoCode());
        System.out.println(country.getName());
        System.out.println(country.getNames().get("zh-CN"));

        // 地区名称
        Subdivision subdivision = response.getMostSpecificSubdivision();
        System.out.println(subdivision.getNames().get("zh-CN"));

        // 城市名称
        City city = response.getCity();
        System.out.println(city.getNames().get("zh-CN"));

        // 邮政编码
        Postal postal = response.getPostal();
        System.out.println(postal.getCode());

        // 经纬度
        Location location = response.getLocation();
        System.out.println(location.getLatitude());
        System.out.println(location.getLongitude());
    }

    public static void main(String[] args) throws Exception {
        String v4IP = getInternetIp();
        System.out.println(v4IP);

    }

    /**
     * 获得内网IP
     *
     * @return 内网IP
     */
    private static String getIntranetIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获得外网IP
     *
     * @return 外网IP
     */
    private static String getV4IP() {
        String ip = "";
        String chinaz = "http://ip.chinaz.com";

        StringBuilder inputLine = new StringBuilder();
        String read;
        URL url;
        HttpURLConnection urlConnection;
        BufferedReader in;
        try {
            url = new URL(chinaz);
            try {
                urlConnection = (HttpURLConnection) url.openConnection();
                urlConnection.setConnectTimeout(TIME_OUT);
                urlConnection.setReadTimeout(TIME_OUT);
                in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(), "UTF-8"));
            } catch (Exception e) {
                //如果超时，则返回内网ip
                return INTRANET_IP;
            }
            while ((read = in.readLine()) != null) {
                inputLine.append(read + "\r\n");
            }
            //System.out.println(inputLine.toString());
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        Pattern p = Pattern.compile("\\<dd class\\=\"fz24\">(.*?)\\<\\/dd>");
        Matcher m = p.matcher(inputLine.toString());
        if (m.find()) {
            String ipstr = m.group(1);
            ip = ipstr;
            //System.out.println(ipstr);
        }
        if ("".equals(ip)) {
            // 如果没有外网IP，就返回内网IP
            return INTRANET_IP;
        }
        return ip;
    }

    /**
     * 获得外网IP
     *
     * @return 外网IP
     */
    private static String getInternetIp() {
        try {
            Enumeration<NetworkInterface> networks = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            Enumeration<InetAddress> addrs;
            while (networks.hasMoreElements()) {
                addrs = networks.nextElement().getInetAddresses();
                while (addrs.hasMoreElements()) {
                    ip = addrs.nextElement();
                    if (ip != null
                            && ip instanceof Inet4Address
                            && ip.isSiteLocalAddress()
                            && !ip.getHostAddress().equals(INTRANET_IP)) {
                        return ip.getHostAddress();
                    }
                }
            }
            // 如果没有外网IP，就返回内网IP
            return INTRANET_IP;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
