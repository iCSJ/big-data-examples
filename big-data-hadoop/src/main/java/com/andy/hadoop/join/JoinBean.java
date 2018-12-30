package com.andy.hadoop.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-30
 **/
public class JoinBean implements Writable {

    private int orderId;

    private String createTime;

    private String productId;

    private int amount;

    private String name;

    private int categoryId;

    private float price;

    private String flag;

    public void set(int orderId, String createTime, String productId, int amount, String name, int categoryId, float price, String flag) {
        this.orderId = orderId;
        this.createTime = createTime;
        this.productId = productId;
        this.amount = amount;
        this.name = name;
        this.categoryId = categoryId;
        this.price = price;
        this.flag = flag;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(orderId);
        output.writeUTF(createTime);
        output.writeUTF(productId);
        output.writeInt(amount);
        output.writeUTF(name);
        output.writeInt(categoryId);
        output.writeFloat(price);
        output.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.orderId = input.readInt();
        this.createTime = input.readUTF();
        this.productId = input.readUTF();
        this.amount = input.readInt();
        this.name = input.readUTF();
        this.categoryId = input.readInt();
        this.price = input.readFloat();
        this.flag = input.readUTF();
    }

    @Override
    public String toString() {
        return "JoinBean{" +
                "orderId=" + orderId +
                ", createTime='" + createTime + '\'' +
                ", productId=" + productId +
                ", amount=" + amount +
                ", name='" + name + '\'' +
                ", categoryId=" + categoryId +
                ", price=" + price +
                ", flag='" + flag + '\'' +
                '}';
    }
}
