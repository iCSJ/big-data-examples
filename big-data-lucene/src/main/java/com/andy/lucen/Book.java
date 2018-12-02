package com.andy.lucen;

/**
 * <p>
 *
 * @author leone
 * @since: 2018-11-23
 **/
public class Book {

    private int bookId;

    private String name;

    private float price;

    private String picture;

    private String description;

    public Book() {
    }

    public Book(int bookId, String name, float price, String picture, String description) {
        this.bookId = bookId;
        this.name = name;
        this.price = price;
        this.picture = picture;
        this.description = description;
    }

    public int getBookId() {
        return bookId;
    }

    public void setBookId(int bookId) {
        this.bookId = bookId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getPicture() {
        return picture;
    }

    public void setPicture(String picture) {
        this.picture = picture;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
