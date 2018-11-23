package com.andy.lucen;

/**
 * <p> lucene测试
 *
 * @author Leone
 * @since 2018-11-22
 **/
public class HelloWorld {


    public static void main(String[] args) {
        Article article = new Article();
        article.setId(10006L);
        article.setAuthor("张三");
        article.setTitle("标题");
        article.setContent("如何才能中彩票");
        article.setUrl("http://www.baidu.com");
        String indexPath = "d:/tmp/words";



    }



}
