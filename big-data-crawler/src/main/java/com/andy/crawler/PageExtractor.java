package com.andy.crawler;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-31
 **/
public class PageExtractor {

    public static void main(String[] args) throws Exception {

        Connection connection = Jsoup.connect("http://v.youku.com/v_show/id_XMzk0NzUzNTUzMg==.html?spm=a2ha1.12325017.m_26665_c_32069.5~5!3~5!2~5~5~A");

        Document doc = connection.get();
//        System.out.println(doc);
        Element qheader = doc.getElementById("qheader");
        System.out.println(qheader);

    }

}
