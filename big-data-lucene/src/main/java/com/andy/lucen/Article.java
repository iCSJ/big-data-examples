package com.andy.lucen;

import org.apache.lucene.document.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;

/**
 * <p>
 *
 * @author leone
 * @since 2018-11-22
 **/
public class Article {

    private Long id;

    private String author;

    private String title;

    private String content;

    private String url;

    public Article() {
    }

    public Article(Long id, String author, String title, String content, String url) {
        this.id = id;
        this.author = author;
        this.title = title;
        this.content = content;
        this.url = url;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Document toDocument(){
        //Lucene存储的格式（Map装的k,v）
        Document doc = new Document();
        //向文档中添加一个long类型的属性，建立索引
        doc.add(new LongPoint("id", id));
        //在文档中存储
        doc.add(new StoredField("id", id));

        //设置一个文本类型，会对内容进行分词，建立索引，并将内容在文档中存储
        doc.add(new TextField("title", title, Field.Store.YES));
        //设置一个文本类型，会对内容进行分词，建立索引，存在文档中存储 / No代表不存储
        doc.add(new TextField("content", content, Field.Store.YES));

        //StringField，不分词，建立索引，文档中存储
        doc.add(new StringField("author", author, Field.Store.YES));

        //不分词，不建立索引，在文档中存储，
        doc.add(new StoredField("url", url));
        return doc;
    }

    public static Article parseArticle(Document doc){
        Long id = Long.parseLong(doc.get("id"));
        String title = doc.get("title");
        String content = doc.get("content");
        String author = doc.get("author");
        String url = doc.get("url");
        Article article = new Article(id, title, content, author, url);
        return article;
    }

    @Override
    public String toString() {
        return "id : " + id + " , title : " + title + " , content : " + content + " , author : " + author + " , url : " + url;
    }

}
