package com.andy.lucen;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;

/**
 * <p>
 *
 * @author Leone
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

    public Document toDocument() {

        Document document = new Document();

        Field id = new TextField("id", getId().toString(), Field.Store.YES);

        Field author = new TextField("author", getAuthor(), Field.Store.YES);

        Field title = new TextField("title", getTitle(), Field.Store.YES);

        Field content = new TextField("content", getContent(), Field.Store.YES);

        Field url = new TextField("url", getUrl(), Field.Store.YES);

        document.add(id);
        document.add(author);
        document.add(title);
        document.add(content);
        document.add(url);

        return document;
    }

}
