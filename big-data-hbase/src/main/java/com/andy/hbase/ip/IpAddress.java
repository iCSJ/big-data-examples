package com.andy.hbase.ip;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;

import java.io.File;
import java.net.InetAddress;
import java.net.URL;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-18
 **/
public class IpAddress {

    public static void main(String[] args) throws Exception {

        // GeoIP2-City 数据库文件
        String path = ClassLoader.getSystemResource("GeoLite2-City.mmdb").getPath();
        File database = new File(path);

        // 创建 DatabaseReader对象
        DatabaseReader reader = new DatabaseReader.Builder(database).build();

        InetAddress ipAddress = InetAddress.getByName("123.125.115.110");

        CityResponse response = reader.city(ipAddress);

        Country country = response.getCountry();
        System.out.println(country.getIsoCode());
        System.out.println(country.getName());
        System.out.println(country.getNames().get("zh-CN"));

        Subdivision subdivision = response.getMostSpecificSubdivision();
        System.out.println(subdivision.getName());
        System.out.println(subdivision.getIsoCode());

        City city = response.getCity();
        // 'Minneapolis'
        System.out.println(city.getName());

        Postal postal = response.getPostal();
        // '55455'
        System.out.println(postal.getCode());

        Location location = response.getLocation();
        System.out.println(location.getLatitude());  // 44.9733
        System.out.println(location.getLongitude()); // -93.2323

    }

}
