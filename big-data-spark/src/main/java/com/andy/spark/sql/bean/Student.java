package com.andy.spark.sql.bean;

import java.io.Serializable;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class Student implements Serializable {

    private static final long serialVersionUID = -8055416696321406779L;

    private Integer id;

    private String name;

    private Integer age;

    public Student() {
    }

    public Student(Integer id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}