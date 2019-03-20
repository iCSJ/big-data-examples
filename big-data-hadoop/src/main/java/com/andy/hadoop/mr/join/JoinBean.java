package com.andy.hadoop.mr.join;

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

    // 订单id
    private Long orderId;

    // 商品id
    private Long productId;

    // 创建时间
    private String createTime;

    // 订单总价
    private Long totalAmount;

    // 商品名称
    private String productName;

    // 商品价格
    private double productPrice;

    // 标记为订单还是商品 0: 商品, 1: 订单
    private String flag;


    public JoinBean() {
    }

    public JoinBean(Long orderId, Long productId, String createTime, Long totalAmount, String productName, double productPrice, String flag) {
        this.orderId = orderId;
        this.productId = productId;
        this.createTime = createTime;
        this.totalAmount = totalAmount;
        this.productName = productName;
        this.productPrice = productPrice;
        this.flag = flag;
    }

    public void set(Long orderId, Long productId, String createTime, Long totalAmount, String productName, double productPrice, String flag) {
        this.orderId = orderId;
        this.productId = productId;
        this.createTime = createTime;
        this.totalAmount = totalAmount;
        this.productName = productName;
        this.productPrice = productPrice;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(orderId);
        output.writeUTF(createTime);
        output.writeLong(totalAmount);
        output.writeLong(productId);
        output.writeUTF(productName);
        output.writeDouble(productPrice);
        output.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.orderId = input.readLong();
        this.createTime = input.readUTF();
        this.totalAmount = input.readLong();
        this.productId = input.readLong();
        this.productName = input.readUTF();
        this.productPrice = input.readDouble();
        this.flag = input.readUTF();
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public Long getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Long totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public double getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(double productPrice) {
        this.productPrice = productPrice;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "{" +
                "orderId=" + orderId +
                ", createTime='" + createTime + '\'' +
                ", totalAmount=" + totalAmount +
                ", productId=" + productId +
                ", productName='" + productName + '\'' +
                ", productPrice=" + productPrice +
                '}';
    }
}
